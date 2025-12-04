# core/discovery.py
"""
Discovery universal para RPPS — heurística + Selenium interativo.
Substitua seu core/discovery.py por este arquivo.
Mantém API pública: crawl_site(base_url), extract_links_from_page(url), selenium_extract_links(url)
"""

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from urllib.parse import urljoin, urlparse
from collections import deque
from .downloader import is_probably_meeting_document # Import the filtering function
import requests
import time
import re
import random
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------
# Config
# ----------------------------
SELENIUM_SCROLL_STEPS = 4
SELENIUM_CLICK_TOP_N = 6
SELENIUM_CLICK_LEVELS = 2
SELENIUM_WAIT_SHORT = 0.6
SELENIUM_WAIT_LONG = 2.5
MAX_CRAWL_DEPTH = 3
MAX_PAGES_FROM_SITE = 300

MAX_REQUEST_RETRIES = 3
REQUEST_TIMEOUT = 20
REQUEST_BACKOFF = (1.5, 3.0)

HEUR_KEYWORDS = [
    "ata", "atas", "atareuni", "ata de", "ata da", "ata do",
    "reuni", "reunião", "reuniao", "comit", "consel", "invest", "investimento",
    "comite de investimento", "atas do comite",
    "publicac", "publica", "document", "documentos", "reunioes", "transpar"
]

BLACKLIST = [
    "diariomunicipal", "diario-oficial", "licitacao", "licitacoes", "edital",
    "concurso", "legislacao", "legislação", "noticia", "noticias",
    # wp-content/uploads removed from blacklist because many sites host PDFs there
]

FILE_EXTS = (".pdf", ".doc", ".docx", ".xls", ".xlsx")

REQUEST_HEADERS = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"},
]

# Selenium options (stealth-ish)
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--log-level=3")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)
chrome_options.add_argument("--disable-blink-features=AutomationControlled")


def init_driver():
    """Create a Selenium webdriver instance. Adjust Service() path if needed in your environment."""
    from selenium.webdriver.chrome.service import Service
    service = Service()  # assumes chromedriver is in PATH
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(30)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception:
        pass
    return driver


# ----------------------------
# Utility
# ----------------------------
def domain_of(url):
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def blacklisted(url):
    if not url:
        return False
    ul = url.lower()
    for b in BLACKLIST:
        if b in ul:
            return True
    return False


def looks_like_file(url):
    if not url:
        return False
    lu = url.lower()
    return any(lu.endswith(ext) for ext in FILE_EXTS)


def is_download_page(url):
    if not url:
        return False
    u = url.lower()
    return "/download" in u or "download" == u.split("/")[-1] or u.endswith("/download/")


# requests with retries + rotating UA
def safe_get_text(url, timeout=REQUEST_TIMEOUT):
    for attempt in range(MAX_REQUEST_RETRIES):
        try:
            headers = random.choice(REQUEST_HEADERS)
            r = requests.get(url, timeout=timeout, verify=False, headers=headers, allow_redirects=True)
            if r.status_code == 200:
                return r.text
        except Exception:
            pass
        time.sleep(random.uniform(*REQUEST_BACKOFF))
    return None


def resolve_redirect_to_file(url, timeout=REQUEST_TIMEOUT):
    """
    Follow redirects (HEAD then GET fallback) and detect if final URL is a file/pdf.
    Returns final file URL if found, else None.
    """
    try:
        headers = random.choice(REQUEST_HEADERS)
        try:
            r = requests.head(url, timeout=timeout, verify=False, headers=headers, allow_redirects=True)
            final_url = r.url
            ct = r.headers.get("Content-Type", "").lower()
            if any(final_url.lower().endswith(ext) for ext in FILE_EXTS) or "application/pdf" in ct or "application/octet-stream" in ct:
                return final_url
        except Exception:
            r = requests.get(url, timeout=timeout, verify=False, headers=headers, allow_redirects=True)
            final_url = r.url
            ct = r.headers.get("Content-Type", "").lower()
            if any(final_url.lower().endswith(ext) for ext in FILE_EXTS) or "application/pdf" in ct or "application/octet-stream" in ct:
                return final_url
    except Exception:
        pass
    return None


# ----------------------------
# Extraction helpers
# ----------------------------
def extract_pdfs_and_file_links_from_html(base_url, html_text):
    found = []
    if not html_text:
        return found
    soup = BeautifulSoup(html_text, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        abs_url = urljoin(base_url, href)
        text = (a.get_text(strip=True) or "").lower()

        if blacklisted(abs_url):
            continue
        
        # Filter based on anchor text using the new filtering function
        if not is_probably_meeting_document(text):
            continue

        # Sempre tenta resolver para uma URL de arquivo direto se parecer um arquivo ou sugerir download
        if looks_like_file(abs_url) or "download" in text or "baixar" in text or "ata nº" in text: # 'ata nº' é um bom indicador
            final_url = resolve_redirect_to_file(abs_url)
            if final_url:
                # Also filter the final URL's filename
                from urllib.parse import unquote
                raw_file_name = urlparse(final_url).path.split('/')[-1]
                decoded_file_name = unquote(raw_file_name)
                if is_probably_meeting_document(decoded_file_name):
                    found.append(final_url)

    for tag in soup.find_all(["iframe", "embed", "object"]):
        src = tag.get("src") or tag.get("data")
        if src:
            abs_url = urljoin(base_url, src)
            if not blacklisted(abs_url) and looks_like_file(abs_url):
                # Para src de iframe/embed, se parece um arquivo, tenta resolver também.
                final_url = resolve_redirect_to_file(abs_url)
                if final_url:
                    # Also filter the final URL's filename
                    from urllib.parse import unquote
                    raw_file_name = urlparse(final_url).path.split('/')[-1]
                    decoded_file_name = unquote(raw_file_name)
                    if is_probably_meeting_document(decoded_file_name):
                        found.append(final_url)
    for p in re.findall(r'https?://[^"\'<>\s]+\.(?:pdf|docx?|xlsx?)', html_text, flags=re.IGNORECASE):
        if not blacklisted(p):
            final_url = resolve_redirect_to_file(p)
            if final_url:
                found.append(final_url)
    # dedupe keeping order
    seen = set()
    out = []
    for u in found:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def element_text_score(text: str, href: str = "") -> int:
    score = 0
    t = (text or "").lower()
    h = (href or "").lower()
    for k in HEUR_KEYWORDS:
        if k in t:
            score += 5
        if k in h:
            score += 4
    if any(x in h for x in ["publica", "public", "document", "docs", "arquivo"]):
        score += 2
    if blacklisted(h):
        score -= 50
    if re.search(r"\b(19|20)\d{2}\b", t):
        score += 8
    if re.search(r"\b(19|20)\d{2}\b", h):
        score += 8
    months = ["jane", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez",
              "january", "feb", "march", "april", "may", "aug", "sep", "oct", "dec"]
    if any(m in t for m in months):
        score += 4
    if any(m in h for m in months):
        score += 4
    if is_download_page(h):
        score += 6
    
    # Pontuação mais agressiva para combinações-chave
    full_text = t + " " + h
    if "ata" in full_text and "comite" in full_text and "invest" in full_text:
        score += 150
    elif "comite" in full_text and "invest" in full_text:
        score += 120
    elif "ata" in full_text and ("comite" in full_text or "conselho" in full_text):
        score += 80
    elif "ata" in full_text:
        score += 50

    return score


# ----------------------------
# Selenium helpers & interaction
# ----------------------------
def expand_all_menus(driver):
    """Try to expand nav/dropdowns via clicks and hover to expose hidden links."""
    try:
        clickable_selectors = [
            "nav *[onclick]", "nav button", "nav .dropdown-toggle",
            "#menu, .menu-toggle, .hamburger", "*[aria-expanded='false']",
            "*[class*='menu']", "*[class*='dropdown']", "[data-toggle='dropdown']",
        ]
        for sel in clickable_selectors:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
            except Exception:
                els = []
            for e in els:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", e)
                    time.sleep(0.04)
                    try:
                        e.click()
                    except Exception:
                        try:
                            driver.execute_script("arguments[0].click();", e)
                        except Exception:
                            pass
                    time.sleep(0.06)
                except Exception:
                    pass
        # hover
        try:
            actions = ActionChains(driver)
            menu_items = driver.find_elements(By.CSS_SELECTOR, "nav li, nav a, .menu li, .menu a")
            for item in menu_items:
                try:
                    actions.move_to_element(item).perform()
                    time.sleep(0.02)
                except Exception:
                    pass
        except Exception:
            pass
    except Exception:
        pass


def wait_dom_change(driver, timeout=6):
    try:
        old = driver.page_source
        end = time.time() + timeout
        while time.time() < end:
            time.sleep(0.12)
            new = driver.page_source
            if new != old:
                return True
    except Exception:
        pass
    return False


def harvest_clickable_elements(driver):
    candidates = []
    try:
        anchors = driver.find_elements(By.TAG_NAME, "a")
        buttons = driver.find_elements(By.TAG_NAME, "button")
        elements = anchors + buttons
        onclicks = driver.find_elements(By.XPATH, "//*[@onclick]")
        role_buttons = driver.find_elements(By.XPATH, "//*[@role='button']")
        elements.extend(onclicks)
        elements.extend(role_buttons)
        possible_card_selectors = [
            "div[class*='card']", "article[class*='card']",
            "div[class*='box']", "div[class*='tile']", "div[class*='item']",
            "li[class*='item']", "div[class*='categoria']",
            "div[class*='post']", "article.post",
        ]

        for sel in possible_card_selectors:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                elements.extend(els)
            except Exception:
                pass

        seen = set()
        for e in elements:
            try:
                href = ""
                try:
                    href = (e.get_attribute("href") or "") or ""
                except Exception:
                    href = ""
                key = (e.tag_name, href, (e.text or "").strip()[:60])
                if key in seen:
                    continue
                seen.add(key)
                if not element_is_visible(driver, e):
                    continue
                text = (e.text or "").strip()
                score = element_text_score(text, href)
                try:
                    rect = driver.execute_script("var r=arguments[0].getBoundingClientRect(); return [r.width, r.height];", e)
                    if rect and isinstance(rect, (list, tuple)) and len(rect) >= 2:
                        width, height = rect[0], rect[1]
                        if width and height and (width > 120 or height > 50):
                            score += 2
                except Exception:
                    pass
                candidates.append((e, score))
            except Exception:
                continue
    except Exception:
        pass
    return sorted(candidates, key=lambda x: x[1], reverse=True)


def element_is_visible(driver, e):
    try:
        if not e.is_displayed():
            return False
        rect = driver.execute_script("var r=arguments[0].getBoundingClientRect(); return r;", e)
        if not rect:
            return False
        width = rect.get("width") if isinstance(rect, dict) else (rect[0] if len(rect) > 0 else 0)
        height = rect.get("height") if isinstance(rect, dict) else (rect[1] if len(rect) > 1 else 0)
        if (width == 0 and height == 0) or width is None or height is None:
            return False
        return True
    except Exception:
        return False


def click_element_safe(driver, e):
    try:
        try:
            ActionChains(driver).move_to_element(e).click().perform()
            return True
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", e)
                return True
            except Exception:
                return False
    except Exception:
        return False


def click_top_candidates_and_extract(driver, base_url, levels=1):
    results = []
    for level in range(levels):
        candidates = harvest_clickable_elements(driver)
        top = [t for t in candidates if t[1] > 0][:SELENIUM_CLICK_TOP_N]
        if not top:
            break
        for e, score in top:
            try:
                driver.execute_script("arguments[0].scrollIntoView({behavior:'auto', block:'center'});", e)
                time.sleep(0.06)
                clicked = click_element_safe(driver, e)
                if not clicked:
                    continue
                wait_dom_change(driver, timeout=SELENIUM_WAIT_LONG)
                html_after = driver.page_source
                results.extend(extract_pdfs_and_file_links_from_html(base_url, html_after))
            except Exception:
                continue
        time.sleep(0.25)
    # dedupe
    seen = set()
    out = []
    for u in results:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ----------------------------
# Special: extract PDFs from pages structured by year/month accordions
# ----------------------------
def selenium_extract_atas_from_year_page(url, driver): # Now accepts driver
    """
    Opens a page that lists atas grouped by years (accordions). It:
    - expands year accordions
    - clicks month links/buttons
    - collects PDF links visible after expansion
    - follows redirect-to-file when necessary
    Returns list of file URLs.
    """
    found = set()
    try:
        # Adiciona uma verificação explícita para garantir que o driver não seja None
        if driver is None:
            raise ValueError("Selenium driver não foi fornecido para selenium_extract_atas_from_year_page.")
        driver.get(url)
        time.sleep(1.0)

        # Expand year accordions
        try:
            anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='#ano'], a[href*='#Ano'], a[href*='#ANO']")
            for a in anchors:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", a)
                    a.click()
                    time.sleep(0.5)
                except Exception:
                    pass

            elements = driver.find_elements(By.CSS_SELECTOR, "*[id^='ano'], *[id*='ano']")
            for el in elements:
                try:
                    txt = (el.text or "").lower()
                    if re.search(r"\b20\d{2}\b", txt) or "ano" in txt:
                        try:
                            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                            el.click()
                            time.sleep(0.5)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

        # Months
        try:
            month_words = [
                "janeiro", "fevereiro", "março", "marco", "abril", "maio",
                "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
                "jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"
            ]

            candidates = driver.find_elements(By.CSS_SELECTOR, "a, button, li")
            for c in candidates:
                try:
                    txt = (c.text or "").strip().lower()
                    if not txt:
                        continue

                    if any(m in txt for m in month_words) or re.search(r"\b20\d{2}\b", txt):

                        try:
                            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", c)
                            c.click()
                            time.sleep(0.8)
                        except Exception:
                            pass

                        # Collect links after click
                        try:
                            links = driver.find_elements(By.TAG_NAME, "a")
                            for a in links:
                                try:
                                    href = a.get_attribute("href")
                                    if not href:
                                        continue
                                    if blacklisted(href):
                                        continue

                                    # Filter based on anchor text
                                    if not is_probably_meeting_document(a.text):
                                        continue

                                    if looks_like_file(href):
                                        if is_probably_meeting_document(urlparse(href).path.split('/')[-1]):
                                            found.add(href)
                                    else:
                                        if "download" in href or "/arquivo" in href or "/files" in href:
                                            try:
                                                final = resolve_redirect_to_file(href)
                                                if final:
                                                    # Filter final URL's filename
                                                    from urllib.parse import unquote
                                                    raw_file_name = urlparse(final).path.split('/')[-1]
                                                    decoded_file_name = unquote(raw_file_name)
                                                    if is_probably_meeting_document(decoded_file_name):
                                                        found.add(final)
                                            except Exception:
                                                pass
                                except Exception:
                                    pass
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

        # Sweep page source
        try:
            html = driver.page_source
            found.update(extract_pdfs_and_file_links_from_html(url, html))
        except Exception:
            pass

        # Try resolving URL itself as download
        try:
            final = resolve_redirect_to_file(url)
            if final:
                found.add(final)
        except Exception:
            pass
    except Exception as e:
        print(f"[DISCOVERY] Erro geral em selenium_extract_atas_from_year_page para a URL {url}: {e}")

    return list(found)

# ----------------------------
# Main Selenium extraction wrapper
# ----------------------------
def selenium_extract_links(base_url):
    """
    Aggressive Selenium-based extraction that:
    - opens page
    - expands menus
    - scrolls
    - clicks promising elements
    - returns file URLs and candidate internal pages
    """
    found = []
    if looks_like_file(base_url):
        return [base_url]

    driver = None
    try:
        driver = init_driver()
        driver.get(base_url)
        time.sleep(0.8)

        expand_all_menus(driver)

        try:
            for i in range(SELENIUM_SCROLL_STEPS):
                driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight*{(i+1)/SELENIUM_SCROLL_STEPS});")
                time.sleep(0.25)
        except Exception:
            pass

        html_before = driver.page_source
        found.extend(extract_pdfs_and_file_links_from_html(base_url, html_before))

        click_results = click_top_candidates_and_extract(driver, base_url, levels=SELENIUM_CLICK_LEVELS)
        found.extend(click_results)

        html_final = driver.page_source
        soup = BeautifulSoup(html_final, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            abs_url = urljoin(base_url, href)
            if blacklisted(abs_url):
                continue
            if looks_like_file(abs_url):
                found.append(abs_url)
            else:
                text = (a.get_text(strip=True) or "").lower()
                if any(k in text for k in HEUR_KEYWORDS) or any(k in abs_url.lower() for k in HEUR_KEYWORDS) or re.search(r"\b(19|20)\d{2}\b", text):
                    found.append(abs_url)

        for tag in soup.find_all(["iframe", "embed", "object"]):
            src = tag.get("src") or tag.get("data")
            if src:
                abs_url = urljoin(base_url, src)
                if looks_like_file(abs_url) and not blacklisted(abs_url):
                    found.append(abs_url)

    except Exception:
        pass
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass

    # dedupe preserving order
    seen = set()
    out = []
    for u in found:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


# ----------------------------
# Static ranking of internal links (requests)
# ----------------------------
def rank_internal_links_by_score(base_domain, raw_links, extra_allowed_domain=None):
    """
    raw_links: list of (abs_url, anchor_text)
    returns prioritized internal candidate URLs (keeps also neutral navigation links)
    """
    BOOST_NAV = ["institucional", "publica", "document", "portal", "menu", "institut", "conselho", "comit", "conselhos"]

    allowed_domains = {base_domain, extra_allowed_domain}
    scored = []
    for abs_url, text in raw_links:
        if domain_of(abs_url) not in allowed_domains:
            continue
        if blacklisted(abs_url):
            continue
        if looks_like_file(abs_url):
            continue

        score = element_text_score(text, abs_url)
        ul = (abs_url or "").lower()
        if any(x in ul for x in BOOST_NAV):
            score += 15 # Aumenta o bônus para garantir que links de navegação importantes sejam seguidos
        scored.append((score, abs_url))

    scored_sorted = sorted(scored, key=lambda x: x[0], reverse=True)
    final = [u for s, u in scored_sorted if s > 10][:40] # Aumenta o score mínimo para filtrar ruído e reduz o limite
    return final


# ----------------------------
# Crawl orchestration
# ----------------------------
def crawl_site(base_url, max_depth=MAX_CRAWL_DEPTH):
    """
    Crawl site entrypoint - Refactored to use a queue for dynamic exploration.
    - Uses a single Selenium driver instance for the entire crawl.
    - Usa ranking que prioriza 'atas' fortemente sobre 'comite'.
    - Se necessário, roda Selenium e reextrai links após cliques/expansões.
    - Usa extractor especializado para páginas de ano/mês de atas.
    """
    all_found_files = []
    to_visit_queue = deque([(base_url, 0)]) # (url, depth)
    visited_urls = set()
    found_primary_target_page = False # Flag para parar a busca após encontrar o alvo principal
    
    base_domain = domain_of(base_url)

    driver = None
    try: # Envolve toda a lógica de crawl em um try para garantir que o finally seja executado
        try:
            driver = init_driver()
        except Exception as e:
            print(f"[DISCOVERY] Falha ao iniciar o Selenium: {e}. O crawler continuará em modo estático.")
            driver = None

        while to_visit_queue and len(visited_urls) < MAX_PAGES_FROM_SITE:
            # Se já encontramos o alvo principal, não precisamos mais explorar outros links de navegação.
            if found_primary_target_page:
                break

            current_url, current_depth = to_visit_queue.popleft()

            if current_url in visited_urls:
                continue
            visited_urls.add(current_url)

            if current_depth > max_depth:
                continue
            if blacklisted(current_url):
                continue

            print(f"[DISCOVERY] Visitando {current_url} (profundidade {current_depth})")

            # 1. Extração de arquivos diretos (estático)
            html_content = safe_get_text(current_url)
            if html_content:
                files_on_page = extract_pdfs_and_file_links_from_html(current_url, html_content)
                for f in files_on_page:
                    if f not in all_found_files:
                        all_found_files.append(f)

            # 2.1. Verificação e uso do extrator especializado para páginas de atas por ano/acordeão
            lower_current_url = current_url.lower()
            # A condição agora é muito mais restrita: a URL deve conter termos-chave.
            # Isso evita que o extrator seja chamado em páginas genéricas.
            is_year_page_candidate = any(k in lower_current_url for k in ["atas", "reunioes", "comite-de-investimentos"])
            
            if driver and is_year_page_candidate:
                print(f"[DISCOVERY] Usando extrator de ano/acordeão especializado em: {current_url}")
                pdfs_from_year_page = selenium_extract_atas_from_year_page(current_url, driver)
                all_found_files.extend([f for f in pdfs_from_year_page if f not in all_found_files])
                found_primary_target_page = True # Marcamos que encontramos e processamos o alvo.
                continue # Se usamos o extrator especializado, podemos pular para a próxima URL da fila.

            # 2. Extração e processamento com Selenium (para conteúdo dinâmico e interação)
            if driver:
                try:
                    driver.get(current_url)
                    time.sleep(1.5) # Espera para JS carregar
                    expand_all_menus(driver)

                    # **NOVO**: Força a interação em páginas de navegação promissoras
                    # Se a página atual é uma página de navegação importante (como 'institucional'),
                    # execute a lógica de clique interativo para encontrar o próximo passo.
                    if any(k in lower_current_url for k in ["institucional", "conselho", "transpar", "investimento"]):
                        print(f"[DISCOVERY] Página promissora. Tentando cliques interativos em: {current_url}")
                        interactive_results = click_top_candidates_and_extract(driver, current_url, levels=1)
                        for f in interactive_results:
                            if f not in all_found_files:
                                all_found_files.append(f)


                    # Tenta mudar para o contexto de um iframe, se houver um promissor
                    current_url_for_join = current_url
                    try:
                        driver.switch_to.default_content() # Garante que estamos no contexto principal
                        # Itera sobre iframes e entra apenas se for do mesmo domínio
                        for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
                            iframe_src = iframe.get_attribute('src')
                            if iframe_src and (domain_of(iframe_src) == base_domain or not domain_of(iframe_src)):
                                try:
                                    print(f"[DISCOVERY] Trocando para o contexto do iframe: {iframe_src}")
                                    driver.switch_to.frame(iframe)
                                    current_url_for_join = iframe_src
                                    break # Sai do loop após encontrar e entrar em um iframe válido
                                except Exception:
                                    continue # Tenta o próximo iframe se este falhar
                    except Exception: pass # Se falhar, continua no contexto principal

                    time.sleep(0.5)
                    
                    html_after_js = driver.page_source
                    soup_after_js = BeautifulSoup(html_after_js, "lxml")
                    
                    # Adiciona arquivos encontrados após JS
                    files_after_js = extract_pdfs_and_file_links_from_html(current_url_for_join, html_after_js)
                    for f in files_after_js:
                        if f not in all_found_files:
                            all_found_files.append(f)

                    # Extrair links de navegação da página renderizada pelo Selenium
                    raw_links_from_selenium = []
                    for a in soup_after_js.find_all("a", href=True):
                        href = a["href"].strip()
                        abs_url = urljoin(current_url_for_join, href)
                        text = a.get_text(strip=True) or ""
                        raw_links_from_selenium.append((abs_url, text))
                    
                    iframe_domain = domain_of(current_url_for_join) if current_url_for_join != current_url else None
                    
                    # Rankear e adicionar novos links à fila
                    new_candidates = rank_internal_links_by_score(base_domain, raw_links_from_selenium, extra_allowed_domain=iframe_domain)
                    for cand_url in new_candidates:
                        # Adiciona verificação para não adicionar URLs com fragmentos se a base já foi visitada
                        base_cand_url = urljoin(cand_url, urlparse(cand_url).path)
                        if cand_url not in visited_urls and base_cand_url not in visited_urls and current_depth + 1 <= max_depth:
                            to_visit_queue.append((cand_url, current_depth + 1))

                except Exception as e:
                    print(f"[DISCOVERY] Erro ao usar Selenium em {current_url}: {e}")
                finally:
                    try:
                        driver.switch_to.default_content() # Volta para o contexto principal
                    except Exception:
                        pass

            # 3. Extração de links de navegação (estático, se Selenium não foi usado ou falhou)
            else: # If driver is None or Selenium failed for this URL
                if html_content:
                    soup_static = BeautifulSoup(html_content, "lxml")
                    raw_links_static = []
                    for a in soup_static.find_all("a", href=True):
                        href = a["href"].strip()
                        abs_url = urljoin(current_url, href)
                        text = a.get_text(strip=True) or ""
                        raw_links_static.append((abs_url, text))
                    
                    new_candidates_static = rank_internal_links_by_score(base_domain, raw_links_static)
                    for cand_url in new_candidates_static:
                        base_cand_url = urljoin(cand_url, urlparse(cand_url).path)
                        if cand_url not in visited_urls and base_cand_url not in visited_urls and current_depth + 1 <= max_depth:
                            to_visit_queue.append((cand_url, current_depth + 1))

    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                print(f"[DISCOVERY] Erro ao fechar o driver do Selenium: {e}")

    # dedupe preserving order and filter blacklist
    seen = set()
    final_files = []
    for u in all_found_files:
        if u not in seen and not blacklisted(u):
            final_files.append(u)
            seen.add(u)

    return final_files

def extract_links_from_page(url):
    return selenium_extract_links(url)


# quick test harness
if __name__ == "__main__":
    # Example usage:
    # test_url = "https://ipi.itajai.sc.gov.br/"
    # print(f"Crawling {test_url}")
    # files = crawl_site(test_url)
    # for f in files:
    #     print(f"Found file: {f}")
    pass # Keep this for the main app.py to call crawl_site
