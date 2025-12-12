"""
Extrator de texto e metadados das atas baixadas
Patchado para ignorar arquivos DOC/DOCX corrompidos ou HTML disfarçado.
"""

from pathlib import Path
import re
import pdfplumber
import docx
from bs4 import BeautifulSoup
import mammoth
import logging
import zipfile
import io

logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("pdfplumber").setLevel(logging.ERROR)
logging.getLogger("pdfminer.layout").setLevel(logging.ERROR)

# palavras pra identificar o tipo de reuniao
KEYWORDS_COMITE = ["comitê de investimentos", "comite de investimentos"]
KEYWORDS_CONSELHO = ["conselho de administração", "conselho administrativo", "conselho fiscal"]

# regex pra capturar datas tipo: 12/04/2024 ou 3 de março de 2025
DATE_PATTERNS = [
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",
    r"\b\d{1,2}\s+de\s+[a-zç]+\s+de\s+\d{4}\b"
]

# -------------------------------------------------------------------
# PDF
# -------------------------------------------------------------------
def extract_text_from_pdf(file_path):
    text = ""
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                text += page_text + "\n"
    except Exception as e:
        print(f"Erro ao extrair texto de {file_path}: {e}")
    return text.strip()

# -------------------------------------------------------------------
# HTML
# -------------------------------------------------------------------
def extract_text_from_html(file_path):
    try:
        content = Path(file_path).read_text(encoding="utf-8", errors="ignore")
        soup = BeautifulSoup(content, "lxml")
        parts = [s for s in soup.stripped_strings]
        return " ".join(parts)
    except Exception as e:
        print(f"Erro ao ler HTML {file_path}: {e}")
        return ""

# -------------------------------------------------------------------
# DOC / DOCX — com validação robusta
# -------------------------------------------------------------------
def extract_text_from_doc(file_path):
    ext = Path(file_path).suffix.lower()

    try:
        # -----------------------------
        # 1) DOCX — validar arquivo ZIP
        # -----------------------------
        if ext == ".docx":
            try:
                with open(file_path, "rb") as f:
                    data = f.read()
                if not zipfile.is_zipfile(io.BytesIO(data)):
                    print(f"[SKIP] DOCX corrompido → {file_path}")
                    return None
            except Exception:
                print(f"[SKIP] Falha ao validar DOCX → {file_path}")
                return None

            # abrir normalmente
            d = docx.Document(str(file_path))
            return "\n".join(p.text for p in d.paragraphs if p.text).strip()

        # -----------------------------
        # 2) DOC — detectar HTML fake
        # -----------------------------
        elif ext == ".doc":
            with open(file_path, "rb") as f:
                head = f.read(300).lower()

            # HTML disfarçado
            if b"<html" in head or b"<!doctype html" in head:
                print(f"[SKIP] .doc é HTML disfarçado → {file_path}")
                return None

            # extrair texto com mammoth
            with open(file_path, "rb") as f:
                result = mammoth.extract_raw_text(f)
            return result.value.strip()

    except Exception as e:
        print(f"Erro ao ler arquivo Word {file_path}: {e}")

    return None

# -------------------------------------------------------------------
# Detectores de metadados simples
# -------------------------------------------------------------------
def detect_meeting_type(text):
    text_lower = text[:500].lower()
    if any(k in text_lower for k in KEYWORDS_COMITE):
        return "Comitê de Investimentos"
    if any(k in text_lower for k in KEYWORDS_CONSELHO):
        return "Conselho"
    return "Desconhecido"

def extract_meeting_date(text):
    text_lower = text[:500].lower()
    for pattern in DATE_PATTERNS:
        m = re.search(pattern, text_lower)
        if m:
            return m.group()
    return "Data não identificada"

# -------------------------------------------------------------------
# Loop principal de metadados
# -------------------------------------------------------------------
def extract_metadata_from_files(downloaded_files, rpps_info=None):
    all_metadata = []

    for entry in downloaded_files:
        file_path = Path(entry["file_path"])
        ext = file_path.suffix.lower()

        # NÃO extraímos mais texto — IA fará isso depois
        text = ""  

        # metadados mínimos
        all_metadata.append({
            "rpps": entry.get("rpps") or (rpps_info["name"] if rpps_info else None),
            "uf": entry.get("uf") or (rpps_info["uf"] if rpps_info else None),
            "file_name": file_path.name,
            "file_path": str(file_path),
            "formato": ext,
            "file_url": entry.get("file_url"),
            "source_page": entry.get("source_page"),
            "tipo_reuniao": None,  # IA vai descobrir
            "data_reuniao": None    # IA vai descobrir
        })

    return all_metadata
