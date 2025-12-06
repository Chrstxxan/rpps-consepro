# core/discovery.py
"""
Discovery universal para RPPS — heurística + Selenium interativo (V3).

Mantém API pública:
- crawl_site(base_url)        → usado pelo app.py
- extract_links_from_page(url)
- selenium_extract_links(url)

Objetivo:
- Encontrar URLs de arquivos ou páginas que listam atas de reuniões
  (comitê de investimentos, conselhos etc.) em sites de RPPS.
- Usar:
  - heurísticas em texto e URL
  - detecção de páginas de download (?cat=7, downloads.php, etc.)
  - Selenium para JS pesado quando necessário
- Ser genérico o bastante pra escalar para ~3.000 sites.
"""

import time
import random
import re
from collections import deque
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

from .downloader import is_probably_meeting_document  # filtro semântico reaproveitado

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------------------------------------------------
# Configurações gerais
# ----------------------------------------------------------------------

MAX_CRAWL_DEPTH = 3          # profundidade máxima de navegação
MAX_PAGES_FROM_SITE = 120    # limite de páginas por domínio

REQUEST_TIMEOUT = 20
MAX_REQUEST_RETRIES = 3
REQUEST_BACKOFF = (1.5, 3.0)

DOC_EXTS = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".htm", ".html")

# Heurística de contexto de atas
HEUR_KEYWORDS = [
    "ata", "atas",
    "reuni", "reunião", "reuniao",
    "comit", "comitê", "comite",
    "invest", "investimento", "investimentos",
    "conselho", "consel", "deliberativo", "fiscal",
]

# URLs que quase nunca interessam para atas
GLOBAL_BLACKLIST_SUBSTR = [
    "diariomunicipal", "diario-oficial", "diariooficial",
    "licitacao", "licitacoes", "pregao", "compras",
    "edital", "concurso",
    "legislacao", "legislação", "lei", "leis",
    "noticia", "notícias", "noticias",
    "ouvidoria",
    "portal-da-transparencia", "portal_transparencia", "transparencia",
    "contato", "fale-conosco", "faleconosco",
    "login", "auth", "sso", "senha",
    "rh", "recursos-humanos",
]

# Links de navegação geral que não queremos clicar com Selenium
NAV_BLACKLIST_TEXT = [
    "portal da transparência", "transparência", "transparencia",
    "ouvidoria", "notícias", "notícias", "noticia", "noticias",
    "legislação", "lei", "leis", "estatuto", "regimento",
    "contato", "fale conosco", "home", "início", "inicio",
    "institucional", "quem somos",
]

# Cabeçalhos HTTP com vários user-agents pra rodar mais “humano”
REQUEST_HEADERS_LIST = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) "
                   "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"},
]


# ----------------------------------------------------------------------
# Helpers básicos
# ----------------------------------------------------------------------

def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def same_domain(a: str, b: str) -> bool:
    return domain_of(a) == domain_of(b)


def url_blacklisted(url: str) -> bool:
    if not url:
        return False
    ul = url.lower()
    return any(b in ul for b in GLOBAL_BLACKLIST_SUBSTR)


def looks_like_file(url: str) -> bool:
    if not url:
        return False
    u = url.lower().split("?", 1)[0]
    return any(u.endswith(ext) for ext in DOC_EXTS)


def is_download_hub_candidate(url: str) -> bool:
    """
    Heurística para páginas tipo "downloads.php?cat=7" (Jaraguá do Sul, etc.):
    - path contendo download(s)/arquivo/documento
    - OU query com chaves tipo cat, categoria, tipo, idCategoria etc.
    """
    if not url:
        return False
    parsed = urlparse(url)
    path = parsed.path.lower()
    qs = parse_qs(parsed.query.lower())

    path_hits = any(x in path for x in ["download", "downloads", "arquivo", "arquivos", "document", "docs", "publica"])
    query_hits = any(k in qs for k in ["cat", "categoria", "idcategoria", "tipo", "idcat"])

    return path_hits and query_hits


def pick_headers():
    return random.choice(REQUEST_HEADERS_LIST)


def safe_get(url: str, timeout: int = REQUEST_TIMEOUT):
    """
    GET com retries + backoff, exclusivo para HTML/texto.
    """
    last_exc = None
    for _ in range(MAX_REQUEST_RETRIES):
        try:
            r = requests.get(url, headers=pick_headers(), timeout=timeout, verify=False, allow_redirects=True)
            if r.status_code == 200 and r.text:
                return r
        except Exception as e:
            last_exc = e
        time.sleep(random.uniform(*REQUEST_BACKOFF))
    if last_exc:
        print(f"[DISCOVERY] Falha GET {url}: {last_exc}")
    return None


# ----------------------------------------------------------------------
# Score de links (priorizar coisas relevantes)
# ----------------------------------------------------------------------

def element_text_score(text: str, href: str) -> int:
    """
    Pontua um link com base no texto anchora + URL.
    """
    score = 0
    t = (text or "").lower()
    h = (href or "").lower()
    full = t + " " + h

    for k in HEUR_KEYWORDS:
        if k in full:
            score += 8

    # páginas que costumam ter docs
    if any(x in h for x in ["ata", "atas", "download", "downloads", "arquivo", "document", "docs"]):
        score += 6

    # anos
    if re.search(r"\b(19|20)\d{2}\b", full):
        score += 6

    # penaliza se for claramente navegação genérica
    if any(x in full for x in ["portal da transparência", "transparência", "transparencia", "ouvidoria", "noticia", "notícias"]):
        score -= 12

    if url_blacklisted(h):
        score -= 20

    return score


# ----------------------------------------------------------------------
# Extração estática de links de documentos e hubs
# ----------------------------------------------------------------------

def extract_docs_from_html(base_url: str, html: str):
    """
    Dado HTML de uma página, retorna:
      - lista de URLs de arquivos (ou páginas-hub altamente candidatas)
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    found = []

    # 1) anchors direto
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        abs_url = urljoin(base_url, href)
        text = a.get_text(strip=True) or ""

        if url_blacklisted(abs_url):
            continue

        # se já parece arquivo direto
        if looks_like_file(abs_url):
            # filtro semântico pelo nome do arquivo + texto
            fname = abs_url.split("/")[-1]
            if is_probably_meeting_document(fname) or is_probably_meeting_document(text):
                found.append(abs_url)
            continue

        # se é link de download/hub
        if "download" in abs_url.lower() or "arquiv" in abs_url.lower():
            # se o texto tem cara de ata/reunião, guardamos a própria página-hub também
            full = (text + " " + abs_url).lower()
            if any(k in full for k in HEUR_KEYWORDS):
                found.append(abs_url)

    # 2) iframes / embeds / objects
    for tag in soup.find_all(["iframe", "embed", "object"]):
        src = tag.get("src") or tag.get("data")
        if not src:
            continue
        abs_url = urljoin(base_url, src)
        if url_blacklisted(abs_url):
            continue
        if looks_like_file(abs_url):
            fname = abs_url.split("/")[-1]
            if is_probably_meeting_document(fname):
                found.append(abs_url)
        elif "download" in abs_url.lower():
            found.append(abs_url)

    # 3) padrões de URL em JS bruto
    for m in re.findall(r'["\'](https?://[^"\']+\.(?:pdf|docx?|xlsx?))["\']', html, flags=re.IGNORECASE):
        if not url_blacklisted(m):
            if is_probably_meeting_document(m.split("/")[-1]):
                found.append(m)

    # 4) Se a página se parece fortemente com um “hub de downloads” (Jaraguá etc),
    #    incluímos a própria página como candidata:
    if is_download_hub_candidate(base_url):
        found.append(base_url)

    # dedupe preservando ordem
    seen = set()
    out = []
    for u in found:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def extract_internal_links(base_url: str, html: str):
    """
    Retorna links internos (mesmo domínio) com score de relevância.
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    base_domain = domain_of(base_url)
    candidates = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        abs_url = urljoin(base_url, href)
        text = a.get_text(strip=True) or ""

        if url_blacklisted(abs_url):
            continue

        if domain_of(abs_url) != base_domain:
            continue

        score = element_text_score(text, abs_url)

        # boost se parece hub (?cat=7, downloads.php etc.)
        if is_download_hub_candidate(abs_url):
            score += 30

        candidates.append((score, abs_url))

    # ordena por score desc e remove repetições
    candidates.sort(key=lambda x: x[0], reverse=True)

    seen = set()
    ordered = []
    for score, url in candidates:
        if url not in seen and score > 0:
            seen.add(url)
            ordered.append((score, url))

    return ordered


# ----------------------------------------------------------------------
# Selenium setup e helpers
# ----------------------------------------------------------------------

def make_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1300,900")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    try:
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        print(f"[DISCOVERY] Falha ao iniciar Chrome Selenium: {e}")
        return None


def selenium_render_and_get_html(driver, url: str):
    try:
        driver.get(url)
        time.sleep(1.5)
        # scroll leve pra carregar lazyload
        for _ in range(3):
            driver.execute_script("window.scrollBy(0, document.body.scrollHeight/3);")
            time.sleep(0.3)
        return driver.page_source
    except Exception as e:
        print(f"[DISCOVERY] Selenium falhou em {url}: {e}")
        return None


def selenium_click_promising_and_collect(driver, base_url: str):
    """
    Clica apenas em elementos que parecem estar ligados a atas/reuniões.
    - NÃO clica em menus genéricos, portal da transparência etc.
    Retorna lista de URLs de docs encontradas após interações.
    """
    out = []
    try:
        elements = driver.find_elements(By.XPATH, "//a|//button")
    except Exception:
        elements = []

    scored = []
    for el in elements:
        try:
            txt = el.text or ""
            href = el.get_attribute("href") or ""
            full = (txt + " " + href).lower()

            # pular se claramente menu genérico
            if any(nb in full for nb in NAV_BLACKLIST_TEXT):
                continue

            score = element_text_score(txt, href)
            if score <= 0:
                continue
            scored.append((score, el, txt, href))
        except Exception:
            continue

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:12]  # não exagerar

    for score, el, txt, href in top:
        try:
            driver.execute_script("arguments[0].scrollIntoView({behavior:'smooth',block:'center'});", el)
            time.sleep(0.2)
            try:
                el.click()
            except Exception:
                try:
                    ActionChains(driver).move_to_element(el).click().perform()
                except Exception:
                    continue
            time.sleep(1.0)
            html_after = driver.page_source
            out.extend(extract_docs_from_html(base_url, html_after))
        except Exception:
            continue

    # dedupe
    seen = set()
    final = []
    for u in out:
        if u not in seen:
            seen.add(u)
            final.append(u)
    return final


# ----------------------------------------------------------------------
# API: extract_links_from_page & selenium_extract_links
# ----------------------------------------------------------------------

def extract_links_from_page(url: str):
    """
    Extração estática simples de links de documentos (sem Selenium).
    Mantida por compatibilidade.
    """
    resp = safe_get(url)
    if not resp:
        return []
    return extract_docs_from_html(url, resp.text)


def selenium_extract_links(url: str):
    """
    Extração mais agressiva em UMA página usando Selenium.
    Mantida por compatibilidade.
    """
    driver = make_driver()
    if not driver:
        return extract_links_from_page(url)

    try:
        html = selenium_render_and_get_html(driver, url)
        found = extract_docs_from_html(url, html or "")
        found.extend(selenium_click_promising_and_collect(driver, url))
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # dedupe
    seen = set()
    result = []
    for u in found:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


# ----------------------------------------------------------------------
# API principal: crawl_site(base_url)
# ----------------------------------------------------------------------

def crawl_site(base_url: str, max_depth: int = MAX_CRAWL_DEPTH):
    """
    Faz crawling BFS no site:
      - só segue links do mesmo domínio
      - prioriza links com forte relação com atas/reuniões/comitê
      - identifica páginas-hub (downloads.php?cat=7 etc.)
      - usa Selenium como fallback quando estático não acha nada
    Retorna lista de URLs de arquivos OU páginas-hub para o downloader.
    """
    base_domain = domain_of(base_url)
    all_found_files = []

    queue = deque([(base_url, 0)])
    visited = set()

    driver = make_driver()  # driver único por domínio (se falhar, fica None)

    while queue and len(visited) < MAX_PAGES_FROM_SITE:
        url, depth = queue.popleft()

        if url in visited:
            continue
        visited.add(url)

        if depth > max_depth:
            continue
        if url_blacklisted(url):
            continue
        if domain_of(url) != base_domain:
            continue

        print(f"[DISCOVERY] Visitando {url} (profundidade {depth})")

        # 1) Tenta HTML estático
        resp = safe_get(url)
        html = resp.text if resp else ""

        # 2) Extrai docs estático
        static_docs = extract_docs_from_html(url, html)
        for d in static_docs:
            if d not in all_found_files:
                all_found_files.append(d)

        # 3) Se não achou nada e URL é muito promissora → tenta Selenium
        looks_promising = any(k in url.lower() for k in ["ata", "reuni", "comit", "invest"])
        if driver and looks_promising and not static_docs:
            html_dyn = selenium_render_and_get_html(driver, url)
            if html_dyn:
                dyn_docs = extract_docs_from_html(url, html_dyn)
                dyn_docs.extend(selenium_click_promising_and_collect(driver, url))
                for d in dyn_docs:
                    if d not in all_found_files:
                        all_found_files.append(d)

        # 4) Descobrir próximos links internos a partir do HTML (estático ou dinâmico se tiver)
        html_for_links = html
        if not html_for_links and driver:
            html_for_links = selenium_render_and_get_html(driver, url) or ""

        internal_scored = extract_internal_links(url, html_for_links)
        for score, next_url in internal_scored:
            if next_url not in visited and depth + 1 <= max_depth:
                queue.append((next_url, depth + 1))

    if driver:
        try:
            driver.quit()
        except Exception:
            pass

    # dedupe final
    seen = set()
    final = []
    for u in all_found_files:
        if u not in seen:
            seen.add(u)
            final.append(u)

    return final
