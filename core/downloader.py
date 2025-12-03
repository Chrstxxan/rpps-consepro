"""
essa parte do sistema vai ser responsavel por baixar arquivos de atas (pdf, doc, html)
a partir dos links descobertos automaticamente.
"""

import time
import random
import hashlib
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
from tqdm import tqdm  # pra mostrar barra de progresso no terminal
import unicodedata
import re
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# lista com varios user-agents reais pra evitar bloqueios por parte dos sites por anti-bot
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
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36"
]

FILENAME_BLACKLIST = [
    "balanc", "demonstr", "extrato",
    "portaria", "resol", "estatuto",
    "membro-", "certificado", "certificacao",
    "certificado-", "lei", "decreto"
]

def normalize_text(s: str) -> str:
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode()
    s = s.lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def is_probably_meeting_document(filename: str) -> bool:
    normalized_name = normalize_text(filename)
    for term in FILENAME_BLACKLIST:
        if term in normalized_name:
            return False
    # só aceita se tiver 'ata' ou 'reuniao'
    if "ata" in normalized_name or "reuniao" in normalized_name or "reunião" in normalized_name:
        return True
    return False

BLACKLIST = ["política de investimentos", "politica de investimentos", "policy"]

def is_blacklisted(s: str) -> bool:
    # retorna True se o link/texto estiver na blacklist
    s = (s or "").lower()
    return any(b in s for b in BLACKLIST)

def get_headers():
    # usa os cabeçalhos aleatorios simulando navegadores diferentes
    return {"User-Agent": random.choice(USER_AGENTS)}

def sha1_bytes(b: bytes) -> str:
    # gera hash SHA1 de bytes pra evitar duplicatas
    return hashlib.sha1(b).hexdigest()

def robust_get(url, retries=3, timeout=15):
    # tenta varias vezes antes de desistir de uma requisicao
    for attempt in range(retries):
        try:
            # tenta com verificação de certificado
            response = requests.get(url, headers=get_headers(), timeout=timeout, verify=True)
            response.raise_for_status()
            return response
        except requests.exceptions.SSLError as ssl_err:
            print(f"⚠️ Erro SSL em {url} ({ssl_err}) — tentativa {attempt+1}, tentando sem verificação...")
            try:
                # refaz ignorando verificação de certificado
                response = requests.get(url, headers=get_headers(), timeout=timeout, verify=False)
                response.raise_for_status()
                return response
            except Exception as e:
                print(f"Erro ao acessar {url} sem verificação SSL ({e}) — tentativa {attempt+1}")
        except Exception as e:
            print(f"Erro ao acessar {url} ({e}) — tentativa {attempt+1}")
            time.sleep(1 + attempt)

    print(f"Falha ao acessar {url}")
    return None

def sanitize_filename(name):
    # remove caracteres invalidos do nome do arquivo
    return "".join(c for c in name if c.isalnum() or c in (' ', '.', '_', '-')).rstrip()

def is_document_link(href):
    # verifica se o link termina com extensoes validas
    if not href:
        return False
    href = href.lower()
    return href.endswith((".pdf", ".doc", ".docx", ".htm", ".html"))

def extract_document_links(page_url):
    """
    Analisa uma página e retorna links diretos para arquivos/documentos.
    Agora aceita links sem extensão e valida pelo Content-Type.
    """
    response = robust_get(page_url)
    if not response:
        return []

    soup = BeautifulSoup(response.text, "lxml")
    found_links = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if is_blacklisted(href):
            continue

        abs_url = urljoin(page_url, href)

        # 🔴 Se o link tem extensão conhecida, aceita direto
        if is_document_link(href):
            found_links.append(abs_url)
            continue

        # 🔴 Se não tem extensão, tenta checar Content-Type via HEAD
        try:
            head_resp = requests.head(abs_url, headers=get_headers(), timeout=10, allow_redirects=True, verify=False)
            ctype = head_resp.headers.get("Content-Type", "").lower()
            if any(t in ctype for t in ["pdf", "msword", "officedocument", "html"]):
                found_links.append(abs_url)
        except Exception:
            pass

    # remove duplicatas mantendo a ordem
    return list(dict.fromkeys(found_links))

def download_files(link_list, out_dir, rpps_info=None):
    # baixa todos os arquivos válidos e retorna lista de metadados
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    downloaded, seen_hashes = [], set()

    for page_link in tqdm(link_list, desc="Baixando arquivos"):
        doc_links = extract_document_links(page_link)

        for doc_url in doc_links:
            try:
                response = robust_get(doc_url)
                if not response:
                    continue

                file_name = sanitize_filename(Path(urlparse(doc_url).path).name)

                # 🔴 Ajusta extensão pelo Content-Type se necessário
                if not file_name.lower().endswith((".pdf", ".doc", ".docx", ".html", ".htm")):
                    ctype = response.headers.get("Content-Type", "").lower()
                    if "pdf" in ctype:
                        file_name += ".pdf"
                    elif "word" in ctype or "officedocument" in ctype:
                        file_name += ".docx"
                    elif "html" in ctype:
                        file_name += ".html"

                dest_path = out_path / file_name

                # 🔴 FILTRO POR NOME DE ARQUIVO (agora com extensão correta)
                if not is_probably_meeting_document(file_name):
                    print(f"Ignorado (não é ata): {file_name}")
                    continue

                if dest_path.exists():
                    continue

                # evita duplicatas por conteúdo
                file_hash = sha1_bytes(response.content)
                if file_hash in seen_hashes:
                    continue
                seen_hashes.add(file_hash)

                # grava o arquivo no disco
                with open(dest_path, "wb") as f:
                    f.write(response.content)

                downloaded.append({
                    "file_path": str(dest_path),
                    "source_page": page_link,
                    "file_url": doc_url,
                    "rpps": rpps_info["name"] if rpps_info else None,
                    "uf": rpps_info["uf"] if rpps_info else None,
                })

            except Exception as e:
                print(f"Erro ao baixar {doc_url}: {e}")

    return downloaded
