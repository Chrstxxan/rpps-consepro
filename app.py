"""
esse é o script principal que integra todas as etapas do projeto:
- busca links das atas
- baixa os arquivos
- extrai os metadados (tipo, data, etc)
- gera relatórios em jsonl e txt organizados
"""

import argparse
import json
from pathlib import Path
from core.discovery import crawl_site
from core.downloader import download_files
from core.extractor import extract_metadata_from_files
from core.metadata import save_metadata
from core.utils import setup_directories

# lista de sites base (pags iniciais dos municipios)
RPPS_SITES = [
    {"name": "IPI Itajaí", "uf": "SC", "url": "https://ipi.itajai.sc.gov.br/atas-comite-de-investimentos"},
    {"name": "IPRESBS São Bento do Sul", "uf": "SC", "url": "https://ipresbs.sc.gov.br/"},
    {"name": "FPMU Umuarama", "uf": "PR", "url": "https://fpmu.umuarama.pr.gov.br/atas-do-comite-apr"},
    {"name": "ISSEM Jaraguá do Sul", "uf": "SC", "url": "https://www.issem.com.br/index.php"},
    {"name": "ISSBLU Blumenau", "uf": "SC", "url": "https://www.issblu.sc.gov.br/pagina/90/comite-de-investimento/sub-pagina/13/"},
    {"name": "IPREVILLE Joinville", "uf": "SC", "url": "https://www.ipreville.sc.gov.br/pagina/56/comite-de-investimentos"},
    {"name": "NavegantesPrev Navegantes", "uf": "SC", "url": "https://www.navegantesprev.sc.gov.br/"}
]

def parse_args():
    parser = argparse.ArgumentParser(description="Coleta Autônoma de Atas de RPPS")
    # agora o default aponta para a pasta "data" dentro do projeto
    project_root = Path(__file__).parent  # pega a pasta do projeto automaticamente
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

    print("Iniciando busca de atas de RPPS...\n")

    all_metadata = []  # guarda todos os metadados consolidados

    for site in RPPS_SITES:
        # adiciona a tag "(Extra)" se o nome do RPPS contiver "umuarama"
        tag = " (Extra)" if "Umuarama" in site["name"] else ""
        print(f"Buscando atas em: {site['name']} ({site['uf']}){tag}")

        # cria o diretorio organizado para o RPPS atual
        base_path = setup_directories(site["name"], site["uf"], base_out)

        # descobre links que contenham atas
        links = crawl_site(site["url"])

        if not links:
            print(f"Nenhuma ata encontrada para {site['name']}. Pulando...\n")
            continue

        # faz o download dos arquivos
        downloaded_files = download_files(links, base_path, site)

        # extrai metadados e analisa tipo e data das reuniões
        metadata = extract_metadata_from_files(downloaded_files, site)

        # acumula todos os metadados pra gerar o relatorio geral no fim
        all_metadata.extend(metadata)

        # define onde salvar os relatorios individuais
        output_dir = base_path / "relatorios"
        output_dir.mkdir(exist_ok=True)

        # salva relatorios em JSONL e TXT (por RPPS)
        save_metadata(metadata, output_dir)

        print(f"Concluído: {site['name']} ({len(downloaded_files)} arquivos)\n")

    # cria os relatorios consolidados (geral de todos os RPPS)
    merged_jsonl = base_out / "atas_geral.jsonl"
    merged_txt = base_out / "atas_geral.txt"

    try:
        # salva o JSONL consolidado
        with open(merged_jsonl, "w", encoding="utf-8") as jf:
            for entry in all_metadata:
                jf.write(json.dumps(entry, ensure_ascii=False) + "\n")

        # salva o TXT consolidado
        with open(merged_txt, "w", encoding="utf-8") as tf:
            tf.write("Relatório geral consolidado de todas as atas coletadas\n")
            tf.write("=" * 80 + "\n\n")
            for entry in all_metadata:
                tf.write(f"RPPS: {entry.get('rpps')} ({entry.get('uf')})\n")
                tf.write(f"Tipo de Reunião: {entry.get('tipo_reuniao')}\n")
                tf.write(f"Data: {entry.get('data_reuniao')}\n")
                tf.write(f"Arquivo: {entry.get('file_name')}\n")
                tf.write(f"Origem: {entry.get('source_page')}\n")
                tf.write(f"Link: {entry.get('file_url')}\n")
                tf.write("-" * 80 + "\n")

        print(f"Relatórios gerais salvos em:\n - {merged_jsonl}\n - {merged_txt}")

    except Exception as e:
        print(f"Erro ao salvar relatório consolidado: {e}")

    print("Processo finalizado com sucesso!")

if __name__ == "__main__":
    main()
