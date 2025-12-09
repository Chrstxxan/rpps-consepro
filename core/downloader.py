"""
Responsável por:
- filtrar links que provavelmente são atas de reunião
- resolver redirects / download.php / content-disposition
- baixar arquivos (pdf, doc, html) de forma robusta
"""

import time
import random
import hashlib
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote
from pathlib import Path
from tqdm import tqdm  # barra de progresso no terminal
import unicodedata
import re
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------------------------------------------------
# User Agents diversos para reduzir chance de bloqueio
# ----------------------------------------------------------------------
USER_AGENTS = [
    # chrome em Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    # edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36 Edg/120.0",
    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    # safari para macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    # android
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36",
]

# ----------------------------------------------------------------------
# Heurística de filtro por nome de arquivo (ATA vs tralha aleatória)
# ----------------------------------------------------------------------
FILENAME_BLACKLIST = [
    "balanc", "demonstr", "extrato", "relatorio", "gestao", "contas", "financeiro", "orcament",
    "portaria", "resolucao", "resolucoes", "estatuto", "regimento", "normativo", "normativos",
    "membro-", "membros", "composicao", "certificado", "certificacao", "certificado-", "credenciamento",
    "lei", "decreto", "norma", "normas", "legislacao", "instrucao",
    "boletim", "informativo", "cartilha", "manual", "tutorial", "guia", "orientacao", "folder",
    "cronograma", "calendario", "recadastramento", "cadastro", "prova-de-vida",
    "planejamento", "politica", "informe", "censo", "organograma", "fluxograma",
    "formulario", "requerimento", "solicitacao", "declaracao",
    "termo", "convenio", "contrato", "licitacao", "edital", "concurso", "adesao",
    "noticia", "noticias", "evento", "publicacao", "revista",
    "gabarito", "resultado", "classificacao", "convocacao",
    "estudo", "atuarial", "governanca",
]

def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode()
    s = s.lower().replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def is_probably_meeting_document(text_to_check: str) -> bool:
    """
    Função usada tanto no discovery quanto aqui para filtrar coisas que NÃO são atas.
    Se não bater em blacklist, a gente considera candidato.
    """
    normalized_name = normalize_text(text_to_check or "")
    for term in FILENAME_BLACKLIST:
        if term in normalized_name:
            return False
    return True

# blacklist por conteúdo textual do link (politica de investimentos etc.)
BLACKLIST_TEXT = ["política de investimentos", "politica de investimentos", "policy"]

def is_blacklisted(s: str) -> bool:
    s = (s or "").lower()
    return any(b in s for b in BLACKLIST_TEXT)

# ----------------------------------------------------------------------
# Utilitários HTTP
# ----------------------------------------------------------------------
def get_headers(referer: str | None = None) -> dict:
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "*/*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    if referer:
        h["Referer"] = referer
    return h

def sha1_bytes(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()

def robust_request(method: str, url: str, referer: str | None = None,
                   retries: int = 3, timeout: int = 20, stream: bool = False,
                   data: dict | None = None):
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.request(
                method,
                url,
                headers=get_headers(referer=referer),
                timeout=timeout,
                verify=False,
                allow_redirects=True,
                stream=stream,
                data=data,
            )
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            print(f"Erro {method} {url} (tentativa {attempt+1}/{retries}): {e}")
            time.sleep(1 + attempt)
    print(f"Falha em {method} {url}: {last_err}")
    return None

# ----------------------------------------------------------------------
# Descoberta de links de documentos dentro de uma página HTML
# ----------------------------------------------------------------------
DOC_EXTS = (".pdf", ".doc", ".docx", ".htm", ".html", ".xlsx", ".xls")

def looks_like_doc_url(href: str) -> bool:
    if not href:
        return False
    href_l = href.lower()
    return any(href_l.split("?", 1)[0].endswith(ext) for ext in DOC_EXTS)

def extract_candidate_doc_urls_from_html(page_url: str, html: str) -> list[str]:
    """
    Varre o HTML procurando links que pareçam documentos.
    - <a href="...pdf">
    - <iframe src="...pdf">
    - padrões tipo window.open("...pdf")
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    candidates: list[str] = []

    # <a href="">
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if is_blacklisted(href):
            continue
        abs_url = urljoin(page_url, href)
        txt = (a.get_text(strip=True) or "").lower()

        # se o nome ou texto parecem reunião / ata, prioriza
        if looks_like_doc_url(href) or "ata" in txt or "reuni" in txt or "comit" in txt:
            candidates.append(abs_url)

    # <iframe>, <embed>, <object>
    for tag in soup.find_all(["iframe", "embed", "object"]):
        src = tag.get("src") or tag.get("data")
        if not src:
            continue
        abs_url = urljoin(page_url, src)
        if looks_like_doc_url(abs_url):
            candidates.append(abs_url)

    # Padrões em JS: "http://...pdf"
    for m in re.findall(r'["\'](https?://[^"\']+\.(?:pdf|docx?|xlsx?|html?))["\']', html, flags=re.IGNORECASE):
        candidates.append(m)

    # dedupe mantendo ordem
    seen = set()
    out = []
    for u in candidates:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

# ----------------------------------------------------------------------
# Nome de arquivo
# ----------------------------------------------------------------------
def sanitize_filename(name: str) -> str:
    return "".join(c for c in name if c.isalnum() or c in (" ", ".", "_", "-")).rstrip()

def filename_from_content_disposition(cd: str | None) -> str | None:
    if not cd:
        return None
    cd = cd.lower()
    # tenta achar filename="..."
    m = re.search(r'filename\*?=([^;]+)', cd, flags=re.IGNORECASE)
    if not m:
        return None
    value = m.group(1).strip().strip('"').strip("'")
    value = unquote(value)
    return sanitize_filename(value)

def guess_filename(url: str, resp: requests.Response) -> str:
    # 1) Content-Disposition vence
    fn = filename_from_content_disposition(resp.headers.get("Content-Disposition"))
    if fn:
        return fn

    # 2) caminho da URL
    path = urlparse(resp.url).path
    base = path.split("/")[-1]
    base = unquote(base)
    base = sanitize_filename(base)
    if base:
        return base

    # 3) fallback por content-type
    ct = (resp.headers.get("Content-Type") or "").lower()
    if "pdf" in ct:
        return "documento.pdf"
    if "word" in ct or "officedocument" in ct:
        return "documento.docx"
    if "html" in ct:
        return "pagina.html"
    return "arquivo.bin"

# ----------------------------------------------------------------------
# Extração de links de documentos (mantemos API antiga, mas mais agressiva)
# ----------------------------------------------------------------------
def extract_document_links(page_url: str) -> list[str]:
    """
    Analisa uma página e retorna links diretos ou fortemente candidatos a arquivos.
    Versão agressiva:
      - lê HTML
      - usa BeautifulSoup
      - tenta HEAD para validar content-type quando não tem extensão
    """
    resp = robust_request("GET", page_url)
    if not resp:
        return []

    html = resp.text
    soup = BeautifulSoup(html, "lxml")
    found_links: list[str] = []

    # 1) candidatos por HTML / JS
    found_links.extend(extract_candidate_doc_urls_from_html(page_url, html))

    # 2) anchors genéricos com validação via HEAD
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if is_blacklisted(href):
            continue
        abs_url = urljoin(page_url, href)

        if looks_like_doc_url(href):
            found_links.append(abs_url)
            continue

        # sem extensão: testa via HEAD
        head_resp = robust_request("HEAD", abs_url, referer=page_url, timeout=10)
        if not head_resp:
            continue
        ctype = (head_resp.headers.get("Content-Type") or "").lower()
        if any(t in ctype for t in ["pdf", "msword", "officedocument", "html"]):
            found_links.append(abs_url)

    # dedupe mantendo ordem
    seen = set()
    out = []
    for u in found_links:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

# ----------------------------------------------------------------------
# Download agressivo de UM link (resolve HTML intermediário, download.php, etc.)
# ----------------------------------------------------------------------
def _download_binary_response(resp: requests.Response,
                              url: str,
                              out_path: Path,
                              rpps_info: dict | None,
                              seen_hashes: set[str]):
    """
    Salva um response binário, faz hash, evita duplicata e monta o dict de retorno.
    """
    file_name = guess_filename(url, resp)
    if not any(file_name.lower().endswith(ext) for ext in (".pdf", ".doc", ".docx", ".html", ".htm", ".xls", ".xlsx")):
        # adiciona extensão básica pelo content-type se faltar
        ct = (resp.headers.get("Content-Type") or "").lower()
        if "pdf" in ct:
            file_name += ".pdf"
        elif "word" in ct or "officedocument" in ct:
            file_name += ".docx"
        elif "html" in ct:
            file_name += ".html"

    # filtro semântico por nome
    if not is_probably_meeting_document(file_name):
        # print(f"DEBUG ignorando por heurística de nome: {file_name}")
        return None

    dest = out_path / file_name
    # evita overwrite burro
    if dest.exists():
        base, ext = dest.stem, dest.suffix
        dest = out_path / f"{base}_{int(time.time())}{ext}"

    content = resp.content
    file_hash = sha1_bytes(content)
    if file_hash in seen_hashes:
        return None
    seen_hashes.add(file_hash)

    dest.write_bytes(content)

    return {
        "file_path": str(dest),
        "file_url": url,
        "source_page": url,  # mantemos a própria URL como origem; o discovery não passa referer
        "rpps": rpps_info["name"] if rpps_info else None,
        "uf": rpps_info["uf"] if rpps_info else None,
    }

def download_single_url(doc_url: str,
                        out_path: Path,
                        rpps_info: dict | None,
                        seen_hashes: set[str],
                        max_html_hops: int = 2):
    """
    Baixa um único "doc_url", mas se receber HTML:
      - tenta achar links para PDFs/Word dentro da página
      - segue esses links (até max_html_hops níveis)
    Isso é o que ajuda a resolver casos tipo download.php, páginas intermediárias etc.
    """
    # 1) tenta baixar o URL original
    resp = robust_request("GET", doc_url, referer=doc_url, stream=False)
    if not resp or not resp.content:
        return None

    ct = (resp.headers.get("Content-Type") or "").lower()

    # 2) se for claramente binário (pdf/doc/etc.), salva direto
    if any(k in ct for k in ["pdf", "word", "officedocument", "octet-stream"]) and "html" not in ct:
        return _download_binary_response(resp, doc_url, out_path, rpps_info, seen_hashes)

    # 3) se parecer HTML, tenta cavar links internos para documentos
    if "html" in ct or "<html" in resp.text[:200].lower():
        html = resp.text

        candidate_links = extract_candidate_doc_urls_from_html(resp.url, html)
        # se não achou nada, tenta a função mais pesada
        if not candidate_links:
            candidate_links = extract_document_links(resp.url)

        for cand in candidate_links[:8]:  # limita um pouco pra não explodir
            sub_resp = robust_request("GET", cand, referer=resp.url, stream=False)
            if not sub_resp or not sub_resp.content:
                continue
            sub_ct = (sub_resp.headers.get("Content-Type") or "").lower()
            if any(k in sub_ct for k in ["pdf", "word", "officedocument", "octet-stream"]) and "html" not in sub_ct:
                return _download_binary_response(sub_resp, cand, out_path, rpps_info, seen_hashes)

        # fallback: se não achou nada mesmo, salvar HTML só pra DEBUG (opcional)
        # (se quiser, pode comentar essa parte)
        # debug_name = out_path / (sanitize_filename(urlparse(doc_url).netloc) + "_debug.html")
        # debug_name.write_text(html, encoding="utf-8", errors="ignore")
        return None

    # 4) se chegou aqui, é algum tipo binário desconhecido, tenta salvar mesmo assim
    return _download_binary_response(resp, doc_url, out_path, rpps_info, seen_hashes)

def download_detail_page(detail_url: str,
                         out_path: Path,
                         rpps_info: dict,
                         seen_hashes: set):
    """
    Resolver genérico para páginas de detalhe do tipo:
        detail://<original_url>|<id>
    Independente do site.
    """

    # ------------------------------------------------------------
    # 0) Decodificar detail://<page_url>|<file_id>
    # ------------------------------------------------------------
    try:
        raw = detail_url.replace("detail://", "")
        page_url, file_id = raw.split("|", 1)
    except:
        print(f"[DOWNLOADER] detail:// inválido: {detail_url}")
        return None

    # ------------------------------------------------------------
    # ⚠️ REGRA UNIVERSAL (SEM HARDCODE):
    # Se a própria page_url já contém id=<file_id>,
    # então NÃO existe página de detalhe intermediária.
    # É um link direto que deve ser tratado como GET.
    # ------------------------------------------------------------
    parsed = urlparse(page_url)
    qs = parsed.query.lower()

    if f"id={file_id}" in qs:
        return download_single_url(page_url, out_path, rpps_info, seen_hashes)

    # ------------------------------------------------------------
    # 1) Baixa a página HTML original
    # ------------------------------------------------------------
    resp = robust_request("GET", page_url)
    if not resp or not resp.text:
        return None
    html = resp.text
    soup = BeautifulSoup(html, "lxml")

    # ------------------------------------------------------------
    # 2) Extrair possíveis endpoints de download
    # ------------------------------------------------------------
    candidates = set()

    # 2.1 forms
    for f in soup.find_all("form"):
        action = f.get("action")
        if action:
            candidates.add(urljoin(page_url, action))

    # 2.2 onclick JS contendo "download"
    onclicks = re.findall(r"['\"]([^'\"]*download[^'\"]*)['\"]", html, flags=re.I)
    for oc in onclicks:
        if ".php" in oc:
            candidates.add(urljoin(page_url, oc))

    # 2.3 window.location / location.href
    locs = re.findall(r"location\s*=\s*['\"]([^'\"]+)['\"]", html, flags=re.I)
    for loc in locs:
        if ".php" in loc:
            candidates.add(urljoin(page_url, loc))

    # 2.4 URLs externas contendo "download"
    urls_js = re.findall(r"https?://[^\"']+(?:download|baixar)[^\"']*", html, flags=re.I)
    for u in urls_js:
        candidates.add(u)

    # ------------------------------------------------------------
    # 3) Heurística genérica (sem mencionar RPPS)
    # ------------------------------------------------------------
    common_paths = [
        "downloads.php",
        "download.php",
        "downloadsget.php",
        "baixar.php",
        "getfile.php",
    ]
    base = page_url.rsplit("/", 1)[0]
    for p in common_paths:
        candidates.add(urljoin(base + "/", p))

    # ------------------------------------------------------------
    # 4) Limpeza dos candidatos (GENÉRICO)
    # ------------------------------------------------------------
    cleaned = set()
    for endpoint in candidates:
        if not endpoint:
            continue

        ep = endpoint.strip()

        # ignora redes sociais
        if any(s in ep for s in [
            "facebook.com", "instagram.com", "twitter.com", "x.com",
            "linkedin.com", "whatsapp.com"
        ]):
            continue

        parsed_ep = urlparse(ep)
        qs_ep = parsed_ep.query.lower()

        # se tem query mas não possui parâmetro de arquivo → é navegação
        has_file_param = any(k in qs_ep for k in ["id=", "codigo=", "file=", "arquivo=", "doc="])
        if qs_ep and not has_file_param:
            continue

        cleaned.add(ep)

    candidates = cleaned
    if not candidates:
        return None

    # ------------------------------------------------------------
    # 5) Tentar POST com payload genérico
    # ------------------------------------------------------------
    payload_variants = [
        {"id": file_id},
        {"codigo": file_id},
        {"file": file_id},
        {"arquivo": file_id},
        {"doc": file_id},
        {"op": "download", "codigo": file_id},
        {"acao": "download", "codigo": file_id},
    ]

    for endpoint in candidates:
        for payload in payload_variants:
            resp_file = robust_request(
                "POST",
                endpoint,
                referer=page_url,
                timeout=20,
                stream=False,
                data=payload,
            )
            if not resp_file:
                continue

            ct = (resp_file.headers.get("Content-Type") or "").lower()
            if any(k in ct for k in ["pdf", "octet-stream", "binary", "msword", "officedocument"]):
                return _download_binary_response(resp_file, endpoint, out_path, rpps_info, seen_hashes)

    return None

# ----------------------------------------------------------------------
# Função principal usada pelo app.py
# ----------------------------------------------------------------------
def download_files(file_urls, out_dir, rpps_info=None):
    """
    Recebe lista de URLs vindas do discovery:
      - URLs diretas de documentos
      - detail://<page>|<id>  (páginas de detalhe para resolver via POST)
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    downloaded = []
    seen_hashes: set[str] = set()

    for doc_url in tqdm(file_urls, desc="Baixando arquivos"):
        try:
            doc_url = str(doc_url)

            # --------------------------------------------------------
            # 0) FILTROS GENÉRICOS PRÉ-DOWNLOAD (não hardcoded)
            # --------------------------------------------------------
            # ignora redes sociais
            if any(s in doc_url for s in [
                "facebook.com", "instagram.com", "twitter.com", "x.com",
                "linkedin.com", "whatsapp.com"
            ]):
                continue

            parsed = urlparse(doc_url)
            path = parsed.path.lower()
            qs = parsed.query.lower()

            # --------------------------------------------------------
            # 1) REGRA ESSENCIAL DO ISSEM (GENÉRICA, SEM HARDCODE):
            #    Se tem id= e NÃO é detail:// → é download direto via GET.
            # --------------------------------------------------------
            if not doc_url.startswith("detail://") and "id=" in qs:
                entry = download_single_url(doc_url, out_path, rpps_info, seen_hashes)
                if entry:
                    downloaded.append(entry)
                continue  # *** NÃO DEIXA CAIR NO POST! ***

            # --------------------------------------------------------
            # 2) Ignora URLs sem extensão que são claramente de navegação
            # --------------------------------------------------------
            if not doc_url.startswith("detail://"):
                if not any(path.endswith(ext) for ext in DOC_EXTS):
                    nav_tokens = ["cat=", "y=", "m=", "ano=", "mes=", "page=", "p="]
                    if any(tok in qs for tok in nav_tokens):
                        continue

            # --------------------------------------------------------
            # 3) DETAIL:// → POST dinâmico
            # --------------------------------------------------------
            if doc_url.startswith("detail://"):
                entry = download_detail_page(doc_url, out_path, rpps_info, seen_hashes)

            else:
                # ----------------------------------------------------
                # 4) GET + follow normal
                # ----------------------------------------------------
                entry = download_single_url(doc_url, out_path, rpps_info, seen_hashes)

            if entry:
                downloaded.append(entry)

        except Exception as e:
            print(f"Erro ao processar {doc_url}: {e}")

    return downloaded
