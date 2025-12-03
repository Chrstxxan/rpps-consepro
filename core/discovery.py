"""
esse modulo vai ser responsavel por descobrir automaticamente links de atas nos sites dos municipios,
sem depender de caminhos fixos. ele vai usar selenium pra lidar com pags dinamicas (tipo ASP.NET)
e beautifulsoup pra analisar o HTML e encontrar keywords relevantes
"""

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from urllib.parse import urljoin
import time
import re
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# configs do selenium pra nao abrir janela (modo headless)
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--log-level=3")  # esconde logs desnecessarios

def init_driver():
    # inicializa o chrome com selenium manager automaticamente
    from selenium.webdriver.chrome.service import Service
    service = Service()
    return webdriver.Chrome(service=service, options=chrome_options)

# lista de palavras que devem ser ignoradas (pra evitar confundir com politica de investimentos)
BLACKLIST = ["política de investimentos", "politica de investimentos", "policy", "institucional", "cidadao", "transparencia", "diariomunicipal"]

def is_blacklisted(text: str) -> bool:
    # retorna True se o texto tiver termos da blacklist
    s = (text or "").lower()
    return any(b in s for b in BLACKLIST)

def crawl_site(base_url: str, wait_time=5, visited=None, depth=0, max_depth=6):
    """
    Navega recursivamente em um site municipal para encontrar links de atas.
    Segue menus como 'institucional', 'transparência', 'investimentos', 'financeiro',
    'comitê', 'ata' e coleta links diretos de documentos.
    """
    if visited is None:
        visited = set()
    if base_url in visited or depth > max_depth:
        return []
    visited.add(base_url)

    print(f"[DISCOVERY] Visitando: {base_url} (profundidade {depth})")

    driver = init_driver()
    try:
        driver.get(base_url)

        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        WebDriverWait(driver, wait_time).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
        )

        # 🔴 Se estamos na página de investimentos, tentar expandir dropdown "Atas"
        if "investimento" in base_url.lower():
            try:
                atas_button = driver.find_element(By.PARTIAL_LINK_TEXT, "Atas")
                atas_button.click()
                time.sleep(2)  # espera abrir o dropdown
            except Exception:
                pass

        html = driver.page_source
    except Exception as e:
        print(f"[DISCOVERY] Erro ao acessar {base_url}: {e}")
        driver.quit()
        return []
    finally:
        driver.quit()

    soup = BeautifulSoup(html, "lxml")
    found_links = []

    keywords_nav = [
        "institucional", "investimento", "financeiro", "investimentos", "publicacoes",
        "documentos", "comitê", "comite", "ata", "atas", "reunião"
    ]

    page_text = soup.get_text(" ", strip=True).lower()

    # 🔴 Se já estamos na página de investimentos e há "atas" no texto, coletar só atas
    if "investimento" in base_url.lower() and ("ata" in page_text or "atas" in page_text):
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            text = a.get_text(strip=True).lower()
            abs_url = urljoin(base_url, href)

            if "ata" in text or "atas" in text or "ata" in abs_url.lower():
                if abs_url.lower().endswith((".pdf", ".doc", ".docx")):
                    found_links.append(abs_url)
                else:
                    # coleta também links sem extensão que parecem ser atas
                    found_links.append(abs_url)
        return list(dict.fromkeys(found_links))

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(strip=True).lower()
        abs_url = urljoin(base_url, href)

        if is_blacklisted(href) or is_blacklisted(text):
            continue

        # 🔴 Se o link/texto contém "atas", coletar documentos diretos e não recursar mais
        if "ata" in text or "atas" in text or "ata" in abs_url.lower():
            if abs_url.lower().endswith((".pdf", ".doc", ".docx")):
                found_links.append(abs_url)
            else:
                found_links.extend(extract_links_from_page(abs_url, wait_time))
            continue

        if abs_url.lower().endswith((".pdf", ".doc", ".docx")):
            found_links.append(abs_url)
            continue

        if any(k in text for k in keywords_nav) or any(k in href.lower() for k in keywords_nav):
            found_links.extend(crawl_site(abs_url, wait_time, visited, depth+1, max_depth))

    return list(dict.fromkeys(found_links))


def extract_links_from_page(url, wait_time=5):
    """Carrega uma página e retorna todos os links encontrados (para coletar atas)."""
    driver = init_driver()
    try:
        driver.get(url)
        time.sleep(wait_time)
        html = driver.page_source
    finally:
        driver.quit()

    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        abs_url = urljoin(url, a["href"].strip())
        if abs_url.lower().endswith((".pdf", ".doc", ".docx")) or "ata" in abs_url.lower():
            links.append(abs_url)
    return links