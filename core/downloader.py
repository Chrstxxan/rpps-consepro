# core/downloader.py
"""
Robust downloader para o projeto RPPS.

- Mantém compatibilidade com a API antiga (download_files, download_files_parallel,
  download_single_url, download_detail_page).
- Não usa asyncio (evita problemas de event loop no Windows).
- Usa ThreadPoolExecutor com limites por domínio para evitar overloading.
- Session por thread para reaproveitar conexões.
- Seen-hashes protegido por lock.
"""

import time
import random
import hashlib
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote
from pathlib import Path
from tqdm import tqdm
import unicodedata
import re
import urllib3
from collections import defaultdict
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------------
# Configuráveis (tune aqui)
# -------------------------
REQUEST_TIMEOUT = 20
REQUEST_RETRIES = 3
REQUEST_BACKOFF = (1.0, 2.0)  # multiplicative backoff factor between retries
MAX_HTML_HOPS = 2
DOC_EXTS = (".pdf", ".doc", ".docx", ".htm", ".html", ".xlsx", ".xls")
DOMAIN_LIMIT = 2          # conexões simultâneas por domínio
GLOBAL_CONCURRENCY = 40   # número MAX de downloads concorrentes no pool
HEAD_TIMEOUT = 10

# ----------------------------------------------------------------------
# User Agents diversos para reduzir chance de bloqueio por bot
# ----------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36 Edg/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
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
    "planejamento", "informe", "censo", "organograma", "fluxograma",
    "formulario", "requerimento", "solicitacao", "declaracao",
    "termo", "convenio", "contrato", "licitacao", "edital", "concurso", "adesao",
    "noticia", "noticias", "evento", "publicacao", "revista",
    "gabarito", "resultado", "classificacao", "convocacao",
    "estudo", "atuarial", "governanca",
]

# -------------------------
# Utilitários e helpers
# -------------------------
def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode()
    s = s.lower().replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def is_probably_meeting_document(text_to_check: str) -> bool:
    """
    Reaproveitada pelo discovery; deve existir para import.
    """
    normalized_name = normalize_text(text_to_check or "")
    for term in FILENAME_BLACKLIST:
        if term in normalized_name:
            return False
    return True

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

def safe_sleep_backoff(attempt: int):
    # jitter + exponential-ish backoff
    base = REQUEST_BACKOFF[0]
    max_factor = REQUEST_BACKOFF[1]
    wait = base * (1 + attempt) * random.uniform(1.0, max_factor)
    time.sleep(wait)

# -------------------------
# Thread-local session
# -------------------------
_thread_local = threading.local()

def get_session():
    """
    Cada thread tem sua requests.Session() para reaproveitar conexões.
    """
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.verify = False
        _thread_local.session = s
    return _thread_local.session

# -------------------------
# Robust request (sync)
# -------------------------
def robust_request(method: str, url: str, referer: str | None = None,
                   retries: int = REQUEST_RETRIES, timeout: int = REQUEST_TIMEOUT,
                   stream: bool = False, data: dict | None = None):
    last_err = None
    session = get_session()
    for attempt in range(retries):
        try:
            resp = session.request(
                method,
                url,
                headers=get_headers(referer=referer),
                timeout=timeout,
                allow_redirects=True,
                stream=stream,
                data=data
            )
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            # print debug line for diagnostics (keeps previous behavior)
            print(f"Erro {method} {url} (tentativa {attempt+1}/{retries}): {e}")
            safe_sleep_backoff(attempt)
    print(f"Falha em {method} {url}: {last_err}")
    return None

# -------------------------
# Link extraction helpers
# -------------------------
def looks_like_doc_url(href: str) -> bool:
    if not href:
        return False
    href_l = href.lower()
    return any(href_l.split("?", 1)[0].endswith(ext) for ext in DOC_EXTS)

def extract_candidate_doc_urls_from_html(page_url: str, html: str) -> list[str]:
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    candidates: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        abs_url = urljoin(page_url, href)
        txt = (a.get_text(strip=True) or "").lower()

        if looks_like_doc_url(href) or "ata" in txt or "reuni" in txt or "comit" in txt:
            candidates.append(abs_url)

    for tag in soup.find_all(["iframe", "embed", "object"]):
        src = tag.get("src") or tag.get("data")
        if not src:
            continue
        abs_url = urljoin(page_url, src)
        if looks_like_doc_url(abs_url):
            candidates.append(abs_url)

    for m in re.findall(r'["\'](https?://[^"\']+\.(?:pdf|docx?|xlsx?|html?))["\']', html, flags=re.IGNORECASE):
        candidates.append(m)

    seen = set()
    out = []
    for u in candidates:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def extract_document_links(page_url: str) -> list[str]:
    resp = robust_request("GET", page_url)
    if not resp:
        return []

    html = resp.text
    soup = BeautifulSoup(html, "lxml")
    found_links: list[str] = []

    found_links.extend(extract_candidate_doc_urls_from_html(page_url, html))

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        abs_url = urljoin(page_url, href)

        if looks_like_doc_url(href):
            found_links.append(abs_url)
            continue

        head_resp = robust_request("HEAD", abs_url, referer=page_url, timeout=HEAD_TIMEOUT)
        if not head_resp:
            continue
        ctype = (head_resp.headers.get("Content-Type") or "").lower()
        if any(t in ctype for t in ["pdf", "msword", "officedocument", "html"]):
            found_links.append(abs_url)

    seen = set()
    out = []
    for u in found_links:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

# -------------------------
# Filename / guessing
# -------------------------
def sanitize_filename(name: str) -> str:
    return "".join(c for c in name if c.isalnum() or c in (" ", ".", "_", "-")).rstrip()

def filename_from_content_disposition(cd: str | None) -> str | None:
    if not cd:
        return None
    # tenta extrair filename e filename*
    m = re.search(r'filename\*?=([^;]+)', cd, flags=re.IGNORECASE)
    if not m:
        return None
    value = m.group(1).strip().strip('"').strip("'")
    value = unquote(value)
    return sanitize_filename(value)

def guess_filename(url: str, resp: requests.Response) -> str:
    fn = filename_from_content_disposition(resp.headers.get("Content-Disposition"))
    if fn:
        return fn

    path = urlparse(resp.url).path
    base = path.split("/")[-1]
    base = unquote(base)
    base = sanitize_filename(base)
    if base:
        return base

    ct = (resp.headers.get("Content-Type") or "").lower()
    if "pdf" in ct:
        return "documento.pdf"
    if "word" in ct or "officedocument" in ct:
        return "documento.docx"
    if "html" in ct:
        return "pagina.html"
    return "arquivo.bin"

# -------------------------
# _download_binary_response
# -------------------------
def _download_binary_response(resp: requests.Response,
                              url: str,
                              out_path: Path,
                              rpps_info: dict | None,
                              seen_hashes: set,
                              seen_hashes_lock: threading.Lock):
    file_name = guess_filename(url, resp)
    if not any(file_name.lower().endswith(ext) for ext in (".pdf", ".doc", ".docx", ".html", ".htm", ".xls", ".xlsx")):
        ct = (resp.headers.get("Content-Type") or "").lower()
        if "pdf" in ct:
            file_name += ".pdf"
        elif "word" in ct or "officedocument" in ct:
            file_name += ".docx"
        elif "html" in ct:
            file_name += ".html"

    if not is_probably_meeting_document(file_name):
        return None

    dest = out_path / file_name
    if dest.exists():
        base, ext = dest.stem, dest.suffix
        dest = out_path / f"{base}_{int(time.time())}{ext}"

    content = resp.content
    file_hash = sha1_bytes(content)

    with seen_hashes_lock:
        if file_hash in seen_hashes:
            return None
        seen_hashes.add(file_hash)

    try:
        dest.write_bytes(content)
    except Exception as e:
        print(f"[ERR_WRITE] {dest}: {e}")
        return None

    return {
        "file_path": str(dest),
        "file_url": url,
        "source_page": url,
        "rpps": rpps_info["name"] if rpps_info else None,
        "uf": rpps_info["uf"] if rpps_info else None,
    }

# -------------------------
# download_single_url
# -------------------------
def download_single_url(doc_url: str, out_path: str, rpps_info, seen_hashes, seen_hashes_lock):
    """
    Baixa um arquivo ou resolve páginas de download indiretas.
    """

    resp = robust_request(
        "GET",
        doc_url,
        referer=rpps_info.get("base_url"),
        stream=False
    )

    if not resp:
        print(f"[DOWNLOAD] Falha GET {doc_url}")
        return None

    ct = (resp.headers.get("Content-Type") or "").lower()

    # ============================================================
    # CASO 0 — WPDM direto (?wpdmdl=xxxx)
    # ============================================================
    if "wpdmdl=" in doc_url.lower():
        return _download_binary_response(
            resp, doc_url, out_path, rpps_info,
            seen_hashes, seen_hashes_lock
        )

    # ============================================================
    # CASO 1 — PDF / DOC direto
    # ============================================================
    if (
        "pdf" in ct
        or any(ext in doc_url.lower() for ext in [".pdf", ".doc", ".docx"])
    ):
        return _download_binary_response(
            resp, doc_url, out_path, rpps_info,
            seen_hashes, seen_hashes_lock
        )

    html = resp.text or ""

    # ============================================================
    # CASO 2 — Servidor devolve HTML
    # ============================================================
    if "html" in ct or "<html" in html[:300].lower():

        # ⚠️ import local para evitar import circular
        from .discovery import extract_docs_from_html

        # -------------------------------------------
        # PATCH 1 — PDF embutido direto no HTML
        # -------------------------------------------
        pdf_links = re.findall(
            r'https?://[^"\']+\.pdf',
            html,
            flags=re.I
        )

        if pdf_links:
            pdf_url = urljoin(doc_url, pdf_links[0])
            sub = robust_request(
                "GET",
                pdf_url,
                referer=doc_url,
                stream=False
            )
            if sub:
                sub_ct = (sub.headers.get("Content-Type") or "").lower()
                if "pdf" in sub_ct:
                    return _download_binary_response(
                        sub, pdf_url, out_path, rpps_info,
                        seen_hashes, seen_hashes_lock
                    )

        # -------------------------------------------
        # PATCH 2 — WPDM dentro do HTML
        # -------------------------------------------
        wpdmdl_links = re.findall(
            r'href=["\']([^"\']+\?wpdmdl=\d+)',
            html,
            flags=re.I
        )

        for w in wpdmdl_links:
            real = urljoin(doc_url, w)
            sub = robust_request(
                "GET",
                real,
                referer=doc_url,
                stream=False
            )
            if sub:
                sub_ct = (sub.headers.get("Content-Type") or "").lower()
                if "pdf" in sub_ct:
                    return _download_binary_response(
                        sub, real, out_path, rpps_info,
                        seen_hashes, seen_hashes_lock
                    )

        # -------------------------------------------
        # PATCH 3 — usar discovery para achar docs
        # -------------------------------------------
        links = extract_docs_from_html(doc_url, html)

        if links:
            real_url = urljoin(doc_url, links[0])
            sub = robust_request(
                "GET",
                real_url,
                referer=doc_url,
                stream=False
            )
            if sub:
                sub_ct = (sub.headers.get("Content-Type") or "").lower()
                if (
                    "pdf" in sub_ct
                    or any(ext in real_url.lower() for ext in [".pdf", ".doc", ".docx"])
                ):
                    return _download_binary_response(
                        sub, real_url, out_path, rpps_info,
                        seen_hashes, seen_hashes_lock
                    )

        print(f"[DOWNLOAD] Nenhum documento válido encontrado em {doc_url}")
        return None

    # ============================================================
    # CASO FINAL — tipo desconhecido
    # ============================================================
    print(f"[DOWNLOAD] Tipo desconhecido em {doc_url} CT={ct}")
    return None

# -------------------------
# download_detail_page
# -------------------------
def download_detail_page(detail_url: str,
                         out_path: Path,
                         rpps_info: dict,
                         seen_hashes: set,
                         seen_hashes_lock: threading.Lock):
    try:
        raw = detail_url.replace("detail://", "")
        page_url, file_id = raw.split("|", 1)
    except Exception:
        print(f"[DOWNLOADER] detail:// inválido: {detail_url}")
        return None

    parsed = urlparse(page_url)
    qs = parsed.query.lower()
    if f"id={file_id}" in qs:
        return download_single_url(page_url, out_path, rpps_info, seen_hashes, seen_hashes_lock)

    resp = robust_request("GET", page_url)
    if not resp or not resp.text:
        return None
    html = resp.text
    soup = BeautifulSoup(html, "lxml")

    candidates = set()

    for f in soup.find_all("form"):
        action = f.get("action")
        if action:
            candidates.add(urljoin(page_url, action))

    onclicks = re.findall(r"['\"]([^'\"]*download[^'\"]*)['\"]", html, flags=re.I)
    for oc in onclicks:
        if ".php" in oc:
            candidates.add(urljoin(page_url, oc))

    locs = re.findall(r"location\s*=\s*['\"]([^'\"]+)['\"]", html, flags=re.I)
    for loc in locs:
        if ".php" in loc:
            candidates.add(urljoin(page_url, loc))

    urls_js = re.findall(r"https?://[^\"']+(?:download|baixar)[^\"']*", html, flags=re.I)
    for u in urls_js:
        candidates.add(u)

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

    cleaned = set()
    for endpoint in candidates:
        if not endpoint:
            continue
        ep = endpoint.strip()
        if any(s in ep for s in ["facebook.com", "instagram.com", "twitter.com", "x.com", "linkedin.com", "whatsapp.com"]):
            continue
        parsed_ep = urlparse(ep)
        qs_ep = parsed_ep.query.lower()
        has_file_param = any(k in qs_ep for k in ["id=", "codigo=", "file=", "arquivo=", "doc="])
        if qs_ep and not has_file_param:
            continue
        cleaned.add(ep)

    candidates = cleaned
    if not candidates:
        return None

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
            resp_file = robust_request("POST", endpoint, referer=page_url, timeout=REQUEST_TIMEOUT, stream=False, data=payload)
            if not resp_file:
                continue
            ct = (resp_file.headers.get("Content-Type") or "").lower()
            if any(k in ct for k in ["pdf", "octet-stream", "binary", "msword", "officedocument"]):
                return _download_binary_response(resp_file, endpoint, out_path, rpps_info, seen_hashes, seen_hashes_lock)

    return None

# -------------------------
# Parallel download engine (keeps the same signature)
# -------------------------
_domain_locks = {}
_domain_locks_lock = threading.Lock()

def _get_domain_semaphore(domain: str):
    # cria semáforo por domínio de forma thread-safe
    if domain in _domain_locks:
        return _domain_locks[domain]
    with _domain_locks_lock:
        if domain not in _domain_locks:
            _domain_locks[domain] = threading.BoundedSemaphore(DOMAIN_LIMIT)
        return _domain_locks[domain]

def get_domain(url: str) -> str:
    return urlparse(url).netloc

def download_files(file_urls, out_dir, rpps_info=None):
    """
    Comportamento síncrono original (itera sequencialmente).
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    downloaded = []
    seen_hashes: set = set()
    seen_hashes_lock = threading.Lock()

    # Garantir lista para tqdm e para reuso
    urls = list(file_urls)

    for doc_url in tqdm(urls, desc="Baixando arquivos"):
        try:
            doc_url = str(doc_url)

            # filtros genéricos
            if any(s in doc_url for s in ["facebook.com", "instagram.com", "twitter.com", "x.com", "linkedin.com", "whatsapp.com"]):
                continue

            parsed = urlparse(doc_url)
            path = parsed.path.lower()
            qs = parsed.query.lower()

            if not doc_url.startswith("detail://") and "id=" in qs:
                entry = download_single_url(doc_url, out_path, rpps_info, seen_hashes, seen_hashes_lock)
                if entry:
                    downloaded.append(entry)
                continue

            if not doc_url.startswith("detail://"):
                if not any(path.endswith(ext) for ext in DOC_EXTS):
                    nav_tokens = ["cat=", "y=", "m=", "ano=", "mes=", "page=", "p="]
                    if any(tok in qs for tok in nav_tokens):
                        continue

            if doc_url.startswith("detail://"):
                entry = download_detail_page(doc_url, out_path, rpps_info, seen_hashes, seen_hashes_lock)
            else:
                entry = download_single_url(doc_url, out_path, rpps_info, seen_hashes, seen_hashes_lock)

            if entry:
                downloaded.append(entry)
        except Exception as e:
            print(f"Erro ao processar {doc_url}: {e}")

    return downloaded

def download_files_parallel(links, out_path, rpps_info=None, workers=8):
    """
    Baixa arquivos em paralelo com deduplicação persistente por conteúdo.
    Evita baixar novamente arquivos já salvos em execuções anteriores,
    independentemente de nome ou URL.
    """

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import json
    import requests

    out_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------
    # HASH INDEX — carregar histórico persistente
    # ------------------------------------------------------------
    hash_index_path = out_path / ".hash_index.json"

    if hash_index_path.exists():
        try:
            with open(hash_index_path, "r", encoding="utf-8") as f:
                persisted_hashes = set(json.load(f).keys())
            print(f"[HASH_INDEX] {len(persisted_hashes)} hashes carregados do histórico")
        except Exception as e:
            print(f"[HASH_INDEX][ERRO] Falha ao ler histórico: {e}")
            persisted_hashes = set()
    else:
        persisted_hashes = set()

    # hashes já vistos (execuções anteriores + atual)
    seen_hashes = set(persisted_hashes)

    downloaded_files = []

    # ------------------------------------------------------------
    # Worker de download individual
    # ------------------------------------------------------------
    def download_one(entry):
        nonlocal seen_hashes

        # entry é uma URL (string)
        url = str(entry)

        try:
            resp = requests.get(url, timeout=20, verify=False)
            if not resp.ok or not resp.content:
                return None

            content = resp.content
            file_hash = sha1_bytes(content)

            # --- DEDUPE GLOBAL (inclui execuções passadas) ---
            if file_hash in seen_hashes:
                print(f"[SKIP][DUPLICADO] {url}")
                return None

            seen_hashes.add(file_hash)

            # nome seguro baseado no hash
            filename = f"doc_{file_hash[:12]}.pdf"
            file_path = out_path / filename

            with open(file_path, "wb") as f:
                f.write(content)

            return {
                "file_path": str(file_path),
                "file_url": url,
                "rpps": rpps_info["name"] if rpps_info else None,
                "uf": rpps_info["uf"] if rpps_info else None,
            }

        except Exception as e:
            print(f"[DOWNLOAD][ERRO] {url}: {e}")
            return None

    # ------------------------------------------------------------
    # Execução paralela
    # ------------------------------------------------------------
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(download_one, link) for link in links]

        for future in as_completed(futures):
            result = future.result()
            if result:
                downloaded_files.append(result)

    # ------------------------------------------------------------
    # Persistir HASH INDEX atualizado
    # ------------------------------------------------------------
    try:
        with open(hash_index_path, "w", encoding="utf-8") as f:
            json.dump(
                {h: True for h in seen_hashes},
                f,
                ensure_ascii=False,
                indent=2
            )
        print(f"[HASH_INDEX] Histórico atualizado ({len(seen_hashes)} hashes)")
    except Exception as e:
        print(f"[HASH_INDEX][ERRO] Falha ao salvar histórico: {e}")

    return downloaded_files

# -------------------------
# Small utility: quick CLI test
# -------------------------
if __name__ == "__main__":
    # quick smoke test (manual)
    import sys
    if len(sys.argv) < 3:
        print("Usage: python downloader.py <out_dir> <url1> [url2 ...]")
        sys.exit(1)
    out = Path(sys.argv[1])
    links = sys.argv[2:]
    print("Starting parallel download of", len(links), "links to", out)
    res = download_files_parallel(links, out, rpps_info=None, workers=8)
    print("Done:", len(res), "files downloaded")
