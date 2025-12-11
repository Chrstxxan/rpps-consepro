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
    {"name": "IPRESBS São Bento do Sul", "uf": "SC", "url": "https://ipresbs.sc.gov.br/atas-reunioes-categorias/comite-de-investimentos/"},
    {"name": "IPI Itajaí", "uf": "SC", "url": "https://ipi.itajai.sc.gov.br/atas-comite-de-investimentos"},
    {"name": "ISSBLU Blumenau", "uf": "SC", "url": "https://www.issblu.sc.gov.br/pagina/90/comite-de-investimento/sub-pagina/13/"},
    {"name": "IPREVILLE Joinville", "uf": "SC", "url": "https://www.ipreville.sc.gov.br/pagina/56/comite-de-investimentos"},
    {"name": "NavegantesPrev Navegantes", "uf": "SC", "url": "https://www.navegantesprev.sc.gov.br/"},
    {"name": "Rio do Sul Prev", "uf": "SC", "url": "https://riodosulprev.sc.gov.br/agenda/visualiza/4"},
    {"name": "SIMPREVI Chapecó", "uf": "SC", "url": "https://www.chapeco.sc.gov.br/simprevi/comite-de-investimentos"},
    {"name": "IPRECON Concórdia", "uf": "SC", "url": "https://iprecon.sc.gov.br/atas-comite-de-investimentos/"},
    {"name": "IBPREV Brusque", "uf": "SC", "url": "https://ibprev.sc.gov.br/atas/?categoria=comite-de-investimentos"},
    {"name": "IPRESF São Francisco do Sul", "uf": "SC", "url": "https://www.ipresf.sc.gov.br/comissao/comissao/documento-auxiliar/7/14/"},
    {"name": "PREVBIGUAÇU Biguaçu", "uf": "SC", "url": "http://prevbiguacu.sc.gov.br/atas-c-i-2018"},
    {"name": "IPRERIO Rio Negrinho", "uf": "SC", "url": "https://www.iprerio.sc.gov.br/comite-investimento"},
    {"name": "INDAPREV Indaial", "uf": "SC", "url": "https://indaprev.com.br/comite-de-investimento/"},
    {"name": "IPASC Caçador", "uf": "SC", "url": "https://www.ipasc.cacador.sc.gov.br/paginas.php?p=5"},
    {"name": "IMPRES Joaçaba", "uf": "SC", "url": "https://impres.sc.gov.br/governanca-corporativa/comite-de-investimentos/atas-de-reunioes/"},
    {"name": "Camboriú", "uf": "SC", "url": "https://previdencia.camboriu.sc.gov.br/documento?DocumentoSearchForm%5Bid_documento_categoria%5D=4&DocumentoSearchForm%5Bq%5D="},
    {"name": "IPRESP Balneário Piçarras", "uf": "SC", "url": "https://ipresp.sc.gov.br/governanca-corporativa/conselho-administrativo/atas-de-reunioes/"},
    {"name": "Pomerode", "uf": "SC", "url": "https://pomerode.atende.net/cidadao/pagina/atas-de-reunioes-do-comite-de-investimentos-fap"},
    {"name": "LAGESPREVI Lages", "uf": "SC", "url": "https://www.lagesprevi.sc.gov.br/admin-financeiro/12/atas-comite-de-investimentos"},
    {"name": "IÇARAPREV Içara", "uf": "SC", "url": "https://icaraprev.sc.gov.br/atas/"},
    {"name": "IPREVE Barra Velha", "uf": "SC", "url": "https://iprevebarravelha.sc.gov.br/artigo/32782/atas-de-reuniao-do-comite-de-investimentos"},
    {"name": "ICPREV Canoinhas", "uf": "SC", "url": "https://icprev.sc.gov.br/conselhos-e-comite/comite-de-investimento"},
    {"name": "IPREVIHO Herval d'Oeste", "uf": "SC", "url": "https://ipreviho.sc.gov.br/transparencia"},
    {"name": "Curitibanos", "uf": "SC", "url": "https://curitibanos.sc.gov.br/estrutura/pagina-6334/pro-gestao/comite-de-investimentos-comin/atas-comin/"},
    {"name": "IPMM Mafra", "uf": "SC", "url": "https://ipmm.sc.gov.br/atas-e-avaliacoes-do-comin"},
    {"name": "TAIOPREV Taió", "uf": "SC", "url": "https://taioprev.sc.gov.br/1-3-2-atas/"},
    {"name": "Itaiópolis", "uf": "SC", "url": "https://itaiopolis.sc.gov.br/"},
    {"name": "Antonio Carlos", "uf": "SC", "url": "https://antoniocarlos.sc.gov.br/pagina-4365/pagina-23129/"},
    {"name": "IPBS Balneário Barra do Sul", "uf": "SC", "url": "https://ipbs.sc.gov.br/?pagina=Atas%20Comite%20de%20Investimentos.php&report=2025"},
    {"name": "IPRECAL Campo Alegre", "uf": "SC", "url": "https://www.iprecal.sc.gov.br/pagina/109/atas-das-reunioes-do-comite-de-investimento"},
    {"name": "Nova Trento", "uf": "SC", "url": "https://novatrento.sc.gov.br/iprevent-comite-de-investimentos/"},
    {"name": "Salto Veloso", "uf": "SC", "url": "https://saltoveloso.sc.gov.br/estrutura/pagina-3955/pagina-27388/"},
    {"name": "RPPS Macieira", "uf": "SC", "url": "https://macieira.sc.gov.br/pagina-12742/"},
    {"name": "FPMU Umuarama", "uf": "PR", "url": "https://fpmu.umuarama.pr.gov.br/atas-do-comite-apr"},
    {"name": "IPREV Santa Catarina", "uf": "SC", "url": "https://www.iprev.sc.gov.br/atas-do-comite-de-investimento/"},
    {"name": "IPPA Palhoça", "uf": "SC", "url": "https://www.ippa.sc.gov.br/publicacoes"}
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
