"""
esse script é de funcoes utilitarias genericas usadas em ttodo o projeto
exemplo: criacao de diretorios organizados por UF e nome do RPPS
"""

from pathlib import Path

def setup_directories(name, uf, base_out):
    # cria o diretório base no formato ./data/UF/Nome_RPPS
    safe_name = name.replace(" ", "_").replace("/", "_")
    path = Path(base_out) / uf / safe_name
    path.mkdir(parents=True, exist_ok=True)
    return path
