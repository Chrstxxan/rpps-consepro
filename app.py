"""
Script principal que integra todas as etapas do projeto:
- discovery paralelo
- download paralelo dos documentos
- extração de metadados
- geração de relatórios consolidados
"""

import argparse
import json
from pathlib import Path

from core.discovery import crawl_site
from core.downloader import download_files_parallel
from core.extractor import extract_metadata_from_files
from core.metadata import save_metadata
from core.utils import setup_directories
from core.parallel_runner import run_discovery_parallel

# lista de sites base (pags de atas dos municipios)
RPPS_SITES = [
    {"name": "INPREVID Videira", "uf": "SC", "url": "https://inprevid.sc.gov.br/"}
]

def parse_args():
    parser = argparse.ArgumentParser(description="Coleta Autônoma de Atas de RPPS")
    project_root = Path(__file__).parent
    parser.add_argument(
        "--out",
        default=str(project_root / "data"),
        help="Diretório base de saída (default: ./data dentro do projeto)"
    )
    return parser.parse_args()

def main():
    # funcao principal que roda o fluxo completo
    args = parse_args()
    base_out = Path(args.out)
    base_out.mkdir(parents=True, exist_ok=True)

    print("\nIniciando discovery paralelo...\n")

    # -------------------------------
    # DISCOVERY PARALELO (fase 1)
    # -------------------------------
    all_sites_links = run_discovery_parallel(
    RPPS_SITES,
    crawl_site,
    4
)

    print("\nDiscovery concluído!\n")

    # -------------------------------
    # DOWNLOAD + METADADOS (fase 2)
    # -------------------------------
    all_metadata = []

    for site_name, links in all_sites_links.items():
        site = next(s for s in RPPS_SITES if s["name"] == site_name)
        print(f"[OK] {site['name']} → {len(links)} links encontrados")

        if not links:
            print(f"Nenhuma ata encontrada para {site['name']}. Pulando...\n")
            continue

        base_path = setup_directories(site["name"], site["uf"], base_out)

        # DOWNLOAD PARALELO
        downloaded_files = download_files_parallel(
            links,
            base_path,
            rpps_info=site,
            workers=6
        )

        # extrai metadados e analisa tipo e data das reuniões
        metadata = extract_metadata_from_files(downloaded_files, site)
        all_metadata.extend(metadata)

        # RELATÓRIO INDIVIDUAL
        out_dir = base_path / "relatorios"
        out_dir.mkdir(exist_ok=True)
        save_metadata(metadata, out_dir)

        print(f"✔ {site['name']} finalizado ({len(downloaded_files)} arquivos)\n")

    # -------------------------------
    # RELATÓRIO CONSOLIDADO
    # -------------------------------
    merged_jsonl = base_out / "atas_geral.jsonl"
    merged_txt = base_out / "atas_geral.txt"

    try:
        with open(merged_jsonl, "w", encoding="utf-8") as jf:
            for entry in all_metadata:
                jf.write(json.dumps(entry, ensure_ascii=False) + "\n")

        with open(merged_txt, "w", encoding="utf-8") as tf:
            tf.write("Relatório geral consolidado de todas as atas coletadas\n")
            tf.write("=" * 80 + "\n\n")
            for entry in all_metadata:
                tf.write(f"RPPS: {entry.get('rpps')} ({entry.get('uf')})\n")
                tf.write(f"Tipo: {entry.get('tipo_reuniao')}\n")
                tf.write(f"Data: {entry.get('data_reuniao')}\n")
                tf.write(f"Arquivo: {entry.get('file_name')}\n")
                tf.write(f"Origem: {entry.get('source_page')}\n")
                tf.write(f"Link: {entry.get('file_url')}\n")
                tf.write("-" * 80 + "\n")

        print(f"Relatórios gerais salvos em:\n - {merged_jsonl}\n - {merged_txt}")

    except Exception as e:
        print(f"Erro ao salvar relatório consolidado: {e}")

    print("\nProcesso finalizado com sucesso!")

if __name__ == "__main__":
    main()
