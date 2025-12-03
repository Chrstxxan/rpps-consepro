"""
essa parte do sistema junta tudo: salva os metadados em .jsonl e .txt
(assim da pra analisar depois com facilidade ou abrir em qualquer editor)
"""

import json
from pathlib import Path
from datetime import datetime

def save_metadata(metadata_list, out_dir):
    # salva os metadados em dois formatos: .jsonl e .txt resumido
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    jsonl_path = out_path / "atas.jsonl"
    txt_path = out_path / "atas_resumo.txt"

    try:
        # salva o jsonl (cada linha é um registro individual)
        with open(jsonl_path, "w", encoding="utf-8") as jf:
            for entry in metadata_list:
                jf.write(json.dumps(entry, ensure_ascii=False) + "\n")

        # salva um resumo legivel em txt (pra abrir facil)
        with open(txt_path, "w", encoding="utf-8") as tf:
            tf.write(f"Relatório gerado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            tf.write("=" * 80 + "\n\n")

            for entry in metadata_list:
                tf.write(f"RPPS: {entry.get('rpps')} ({entry.get('uf')})\n")
                tf.write(f"Tipo de Reunião: {entry.get('tipo_reuniao')}\n")
                tf.write(f"Data: {entry.get('data_reuniao')}\n")
                tf.write(f"Arquivo: {entry.get('file_name')}\n")
                tf.write(f"Origem: {entry.get('source_page')}\n")
                tf.write(f"Link: {entry.get('file_url')}\n")
                tf.write("-" * 80 + "\n")

        print(f"Metadados salvos em:\n - {jsonl_path}\n - {txt_path}")

    except Exception as e:
        print(f"Erro ao salvar metadados: {e}")
