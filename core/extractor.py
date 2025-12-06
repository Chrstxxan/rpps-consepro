"""
essa parte do sistema abre os arquivos baixados, extrai o texto e identifica
informacoes uteis (data da reuniao, tipo de reuniao, UF e tal)
salva tambem os textos em .txt pra analise posterior
"""

from pathlib import Path
import re
import pdfplumber
import docx
from bs4 import BeautifulSoup
import mammoth  # usando para ler arquivos .doc antigos (compatível com todos os OS)
import logging

logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("pdfplumber").setLevel(logging.ERROR) # essas linhas ignoram os warnings de renderizacao dos padroes
logging.getLogger("pdfminer.layout").setLevel(logging.ERROR) #graficos dos pdf's

# palavras pra identificar o tipo de reuniao
KEYWORDS_COMITE = ["comitê de investimentos", "comite de investimentos"]
KEYWORDS_CONSELHO = ["conselho de administração", "conselho administrativo", "conselho fiscal"]

# regex pra capturar datas tipo: 12/04/2024 ou 3 de março de 2025
DATE_PATTERNS = [
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",
    r"\b\d{1,2}\s+de\s+[a-zç]+\s+de\s+\d{4}\b"
]

def extract_text_from_pdf(file_path):
    # extrai texto de arquivos PDF usando pdfplumber
    text = ""
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                text += page_text + "\n"
    except Exception as e:
        print(f"Erro ao extrair texto de {file_path}: {e}")
    return text.strip()

def extract_text_from_html(file_path):
    # extrai texto de arquivos html de forma compativel com qualquer versão do bs4
    try:
        content = Path(file_path).read_text(encoding="utf-8", errors="ignore")
        soup = BeautifulSoup(content, "lxml")
        parts = []
        for s in soup.stripped_strings:
            parts.append(s)
        return " ".join(parts)
    except Exception as e:
        print(f"Erro ao ler HTML {file_path}: {e}")
        return ""

def extract_text_from_doc(file_path):
    # extrai texto de arquivos .docx (Word novo) ou .doc (Word antigo)
    ext = Path(file_path).suffix.lower()
    try:
        if ext == ".docx":
            d = docx.Document(str(file_path))
            return "\n".join(p.text for p in d.paragraphs if p.text).strip()

        elif ext == ".doc":
            with open(file_path, "rb") as f:
                result = mammoth.extract_raw_text(f)
            return result.value.strip()

    except Exception as e:
        print(f"Erro ao ler arquivo Word {file_path}: {e}")
    return ""

def detect_meeting_type(text):
    """
    Identifica se o texto pertence a comite, conselho ou é desconhecido.
    A busca agora é limitada aos primeiros 500 caracteres para maior precisão.
    """
    text_lower = text[:500].lower() # Analisa apenas o início do documento
    if any(k in text_lower for k in KEYWORDS_COMITE):
        return "Comitê de Investimentos"
    if any(k in text_lower for k in KEYWORDS_CONSELHO):
        return "Conselho"
    return "Desconhecido"

def extract_meeting_date(text):
    """
    Procura uma data válida no conteúdo textual.
    A busca agora é limitada aos primeiros 500 caracteres para evitar datas irrelevantes.
    """
    text_lower = text[:500].lower() # Analisa apenas o início do documento
    for pattern in DATE_PATTERNS:
        match = re.search(pattern, text_lower)
        if match:
            return match.group()
    return "Data não identificada"

def extract_metadata_from_files(downloaded_files, rpps_info=None):
    # le todos os arquivos baixados, extrai o texto e gera metadados estruturados
    all_metadata = []

    for entry in downloaded_files:
        file_path = Path(entry["file_path"])
        ext = file_path.suffix.lower()

        # extrai o texto dependendo do tipo de arquivo
        if ext == ".pdf":
            text = extract_text_from_pdf(file_path)
        elif ext in [".html", ".htm"]:
            text = extract_text_from_html(file_path)
        elif ext in [".docx", ".doc"]:
            text = extract_text_from_doc(file_path)
        else:
            text = ""

        # analisa o texto pra identificar tipo e data
        meeting_type = detect_meeting_type(text)
        meeting_date = extract_meeting_date(text)

                # filtro extra: se não parece ata / reunião, descarta cedo
        header = (text[:400] or "").lower()
        if meeting_type == "Desconhecido" and "ata" not in header and "reuni" not in header:
            # pula esse arquivo, provavelmente é código de ética, relatório etc.
            continue


        # salva o texto extraido em .txt pra analise posterior
        try:
            txt_path = file_path.with_suffix(".txt")
            txt_path.write_text(text or "", encoding="utf-8")
        except Exception as e:
            print(f"Erro ao salvar TXT de {file_path}: {e}")

        # adiciona os metadados a lista final
        all_metadata.append({
            "rpps": entry.get("rpps") or (rpps_info["name"] if rpps_info else None),
            "uf": entry.get("uf") or (rpps_info["uf"] if rpps_info else None),
            "file_name": file_path.name,
            "file_path": str(file_path),
            "formato": ext,
            "file_url": entry.get("file_url"),
            "source_page": entry.get("source_page"),
            "tipo_reuniao": meeting_type,
            "data_reuniao": meeting_date
        })

    return all_metadata
