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
    {"name": "IPMC Curitiba", "uf": "PR", "url": "https://previdencia.curitiba.pr.gov.br/transparencia/11"}, 
    {"name": "FPMA Araucária", "uf": "PR", "url": "https://www.fpma.com.br/?pag=ConteudoVP&itm=263"},
    {"name": "PREVSJP São José dos Pinhais", "uf": "PR", "url": "https://prev.sjp.pr.gov.br/atas-do-cominvest/"}, 
    {"name": "MaringaPrevidencia", "uf": "PR", "url": "https://www.maringaprevidencia.com.br/ata"},
    {"name": "ParanaguáPREV", "uf": "PR", "url": "https://www.paranaguaprev.com.br/publicacoes/?idCategoriaPublicacao=61"},
    {"name": "GuaraPREV Guarapuava", "uf": "PR", "url": "https://guaraprev.com.br/governanca-corporativa/comite-de-investimentos/atas-de-reunioes/"},
    {"name": "CAPREV Cascavel", "uf": "PR", "url": "https://caprevcascavel.com.br/governanca-corporativa/comite-de-investimentos/atas-de-reunioes/"},
    {"name": "TOLEDOPREV Toledo", "uf": "PR", "url": "https://toledoprev.toledo.pr.gov.br/institucional/comite-de-investimentos/atas-documentos"},
    {"name": "FOZPREV Foz do Iguaçu", "uf": "PR", "url": "https://fozprev.pmfi.pr.gov.br/#/site/categorias/menu/70"}, 
    {"name": "FAZPREV Fazenda Rio Grande", "uf": "PR", "url": "https://fazprev.pr.gov.br/atas-das-reunioes-do-comite-de-investimentos/"},
    {"name": "Previdencia Campo Largo", "uf": "PR", "url": "https://previdenciacampolargo.atende.net/transparencia/grupo/outras-publicacoes?agg=eyJncnVwbyI6IjI1IiwiaWQiOiIyNF8yIn0%3D"}, 
    {"name": "Colombo Previdencia", "uf": "PR", "url": "https://www.colomboprevidencia.com.br/ata-investimento/"},
    {"name": "Pinhais Previdencia", "uf": "PR", "url": "https://pinhaisprevidencia.atende.net/cidadao/pagina/atas-comite-de-investimentos"},
    {"name": "IPPASA Arapongas", "uf": "PR", "url": "https://arapongas.atende.net/subportal/ippasa#arquivos"} 
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
            workers=8
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
