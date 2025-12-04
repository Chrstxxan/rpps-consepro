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
    "balanc", "demonstr", "extrato", "relatorio", "gestao", "contas", "financeiro", "orcament",
    "portaria", "resolucao", "resolucoes", "estatuto", "regimento", "normativo",
    "membro-", "membros", "composicao", "certificado", "certificacao", "certificado-", "credenciamento",
    "lei", "decreto", "norma", "normas", "legislacao", "instrucao",
    "boletim", "informativo", "cartilha", "manual", "tutorial", "guia", "orientacao", "folder",
    "cronograma", "calendario", "recadastramento", "cadastro", "prova-de-vida",
    "planejamento", "politica", "informe", "censo", "organograma", "fluxograma",
    "formulario", "requerimento", "solicitacao", "declaracao",
    "termo", "convenio", "contrato", "licitacao", "edital", "concurso", "adesao",
    "noticia", "noticias", "evento", "publicacao", "revista",
    "gabarito", "resultado", "classificacao", "convocacao",
    "estudo", "atuarial", "governanca"
]

def normalize_text(s: str) -> str:
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode()
    s = s.lower().replace('-', ' ') # Replace hyphens with spaces for better matching
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def is_probably_meeting_document(text_to_check: str) -> bool:
    normalized_name = normalize_text(text_to_check)
    for term in FILENAME_BLACKLIST:
        if term in normalized_name:
            return False
    # Se passou pela blacklist, consideramos relevante, pois o discovery já filtrou o contexto.
    return True

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

def download_files(file_urls, out_dir, rpps_info=None):
    from urllib.parse import urlparse
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    downloaded, seen_hashes = [], set()

    for doc_url in tqdm(file_urls, desc="Baixando arquivos"):
        try:
            response = robust_get(doc_url)
            if not response or not response.content:
                continue

            # Decodifica o nome do arquivo da URL (ex: %C3%A7 -> ç) antes de sanitizar e remover caracteres inválidos
            from urllib.parse import unquote
            raw_file_name = urlparse(doc_url).path.split('/')[-1]
            decoded_file_name = unquote(raw_file_name)
            file_name = sanitize_filename(decoded_file_name)
            if not file_name: # Se a URL não tem nome de arquivo (ex: /download.php?id=1)
                file_name = hashlib.md5(doc_url.encode()).hexdigest()

            if not any(file_name.lower().endswith(ext) for ext in (".pdf", ".doc", ".docx", ".html", ".htm")):
                ctype = response.headers.get("Content-Type", "").lower()
                if "pdf" in ctype: file_name += ".pdf"
                elif "word" in ctype or "officedocument" in ctype: file_name += ".docx"
                elif "html" in ctype: file_name += ".html"

            if not is_probably_meeting_document(file_name):
                # print(f"DEBUG: Ignorando arquivo por blacklist: {file_name}") # Descomente para depurar
                continue

            dest_path = out_path / file_name
            if dest_path.exists(): continue

            file_hash = sha1_bytes(response.content)
            if file_hash in seen_hashes: continue
            seen_hashes.add(file_hash)

            dest_path.write_bytes(response.content)

            downloaded.append({
                "file_path": str(dest_path),
                "source_page": doc_url, # A própria URL é a fonte
                "file_url": doc_url,
                "rpps": rpps_info["name"] if rpps_info else None,
                "uf": rpps_info["uf"] if rpps_info else None,
            })
        except Exception as e:
            print(f"Erro ao processar {doc_url}: {e}")

    return downloaded
