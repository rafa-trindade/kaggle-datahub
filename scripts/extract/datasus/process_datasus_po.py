import os
import logging

import datasus_dbc
from dbfread import DBF
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(file_path)

scripts_dir = os.path.dirname(os.path.dirname(current_dir))
base_dir = os.path.dirname(scripts_dir)

dbc_dir = os.path.join(base_dir, "data", "utils", "dbc_datasus_po")
parquet_dir = os.path.join(base_dir, "data", "raw")

os.makedirs(parquet_dir, exist_ok=True)

parquet_final_path = os.path.join(parquet_dir, "raw_painel_de_oncologia.parquet")

arquivos_dbc = [f for f in os.listdir(dbc_dir) if f.lower().endswith(".dbc")]
arquivos_dbc.sort()

if not arquivos_dbc:
    logger.warning("Nenhum arquivo .DBC encontrado em data/utils/dbc_datasus_po.")
    raise SystemExit(0)

logger.info("Iniciando processamento dos arquivos DBC para Parquet único...")

writer = None
total_geral = 0

try:
    for arquivo in arquivos_dbc:
        caminho_dbc = os.path.join(dbc_dir, arquivo)
        nome_base = os.path.splitext(arquivo)[0]
        caminho_dbf = os.path.join(dbc_dir, f"{nome_base}.dbf")

        logger.info(f"=== Arquivo: {arquivo} ===")
        logger.info(f"Descompactando {arquivo}...")

        try:
            datasus_dbc.decompress(caminho_dbc, caminho_dbf)
        except Exception as e:
            logger.error(f"Falha ao descompactar {arquivo}: {e}")
            continue

        logger.info(f"Lendo {nome_base}.dbf e escrevendo no Parquet final em chunks...")

        tabela = DBF(caminho_dbf, encoding="latin-1")

        batch_registros = []
        batch_size = 50_000
        count_total = 0

        for registro in tabela:
            count_total += 1
            total_geral += 1
            batch_registros.append(registro)

            if len(batch_registros) >= batch_size:
                df_batch = pd.DataFrame(batch_registros)
                table = pa.Table.from_pandas(df_batch, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(parquet_final_path, table.schema)

                writer.write_table(table)
                batch_registros = []

        if batch_registros:
            df_batch = pd.DataFrame(batch_registros)
            table = pa.Table.from_pandas(df_batch, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(parquet_final_path, table.schema)

            writer.write_table(table)

        logger.info(f"Registros totais em {arquivo}: {count_total}")

        if os.path.exists(caminho_dbf):
            os.remove(caminho_dbf)
            logger.info(f"Arquivo temporário {caminho_dbf} removido.")

finally:
    if writer is not None:
        writer.close()
        logger.info(f"Parquet final salvo em: {parquet_final_path}")

logger.info("Processamento concluído!")
logger.info(f"Total de registros em todos os arquivos: {total_geral}")
logger.info(f"Arquivo único gerado: {parquet_final_path}")
