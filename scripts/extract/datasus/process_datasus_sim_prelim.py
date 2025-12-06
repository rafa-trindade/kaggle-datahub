import os
import logging
import csv
import time
import datasus_dbc
from dbfread import DBF
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------
# Diretórios
# -----------------------------

file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(file_path)

scripts_dir = os.path.dirname(os.path.dirname(current_dir))
base_dir = os.path.dirname(scripts_dir)

DBC_DIR = os.path.join(base_dir, "data", "utils", "dbc_datasus_sim", "prelim")
PARQUET_FINAL_PATH = os.path.join(base_dir, "data", "raw", "raw_sistema_info_mortalidade_prelim.parquet")
CSV_TEMP_PATH = os.path.join(base_dir, "data", "raw", "raw_sistema_info_mortalidade_prelim_temp.csv")

os.makedirs(os.path.join(base_dir, "data", "raw"), exist_ok=True)

# -----------------------------
# Variáveis de controle
# -----------------------------
batch_size = 50_000
total_registros = 0
colunas_finais_dinamicas = set()
parquet_writer = None

# -----------------------------
# Seleção dos arquivos DBC
# -----------------------------
arquivos_dbc = [f for f in os.listdir(DBC_DIR) if f.lower().endswith(".dbc")]
arquivos_dbc.sort()

if not arquivos_dbc:
    logger.warning(f"Nenhum arquivo .dbc encontrado em {DBC_DIR}.")
    
logger.info(f"Arquivos encontrados: {len(arquivos_dbc)}")

logger.info("Fase 1: Construindo o Union Schema (Coletando nomes de todas as colunas)...")
try:
    for idx, arquivo in enumerate(arquivos_dbc, 1):
        caminho_dbc = os.path.join(DBC_DIR, arquivo)
        caminho_dbf = caminho_dbc.replace(".DBC", ".DBF").replace(".dbc", ".dbf")
        
        try:
            if os.path.exists(caminho_dbf): os.remove(caminho_dbf)
            datasus_dbc.decompress(caminho_dbc, caminho_dbf) 
            tabela = DBF(caminho_dbf, encoding="latin1")
            
            colunas_finais_dinamicas.update(tabela.field_names)
            
            os.remove(caminho_dbf)
        except Exception as e:
            logger.error(f"❌ Falha na Fase 1 para {arquivo}: {e}")
            if os.path.exists(caminho_dbf): os.remove(caminho_dbf)
            continue
    
    colunas_finais_dinamicas = sorted(list(colunas_finais_dinamicas))
    logger.info(f"Union Schema concluído. Total de colunas: {len(colunas_finais_dinamicas)}")

except Exception as e:
    logger.critical(f"Falha irrecuperável na Fase 1 de Union Schema: {e}")
    exit(1)


logger.info("Fase 2: Processando e escrevendo dados diretamente em Parquet (Batch Mode)...")

try:
    if not colunas_finais_dinamicas:
        logger.warning("Union Schema vazio. Finalizando.")
        raise Exception("Nenhuma coluna detectada para processamento.")

    parquet_schema = pa.schema([(name, pa.string()) for name in colunas_finais_dinamicas])
    parquet_writer = pq.ParquetWriter(PARQUET_FINAL_PATH, parquet_schema)

    for idx, arquivo in enumerate(arquivos_dbc, 1):
        caminho_dbc = os.path.join(DBC_DIR, arquivo)
        caminho_dbf = caminho_dbc.replace(".DBC", ".DBF").replace(".dbc", ".dbf")

        logger.info(f"[{idx}/{len(arquivos_dbc)}] Processando {arquivo}...")
        
        try:
            if os.path.exists(caminho_dbf): os.remove(caminho_dbf)
            datasus_dbc.decompress(caminho_dbc, caminho_dbf)
        except Exception as e:
            logger.error(f"❌ Falha na descompactação na Fase 2 para {arquivo}: {e}")
            continue

        tabela = DBF(caminho_dbf, encoding="latin1")
        batch = []
        count = 0
        
        for registro in tabela:
            count += 1
            total_registros += 1

            registro_formatado = {k: str(registro.get(k, "")).strip() for k in colunas_finais_dinamicas}
            batch.append(registro_formatado)

            if len(batch) >= batch_size:
                df = pd.DataFrame(batch, columns=colunas_finais_dinamicas, dtype=str)
                table = pa.Table.from_pandas(df, schema=parquet_schema, preserve_index=False)
                
                parquet_writer.write_table(table)
                batch.clear()

        if batch:
            df = pd.DataFrame(batch, columns=colunas_finais_dinamicas, dtype=str)
            table = pa.Table.from_pandas(df, schema=parquet_schema, preserve_index=False)
            parquet_writer.write_table(table)
            batch.clear()

        logger.info(f"{arquivo}: {count} registros processados.")

        if os.path.exists(caminho_dbf):
            os.remove(caminho_dbf)
            logger.info(f"DBF temporário removido: {caminho_dbf}")
            
except Exception as main_e:
    logger.error(f"Ocorreu um erro principal durante o processamento: {main_e}")
finally:
    if parquet_writer:
        parquet_writer.close()
    
    if os.path.exists(CSV_TEMP_PATH):
        os.remove(CSV_TEMP_PATH)
        logger.info(f"CSV temporário {CSV_TEMP_PATH} removido (não mais usado).")


# -----------------------------
# Conclusão
# -----------------------------
if total_registros > 0:
    logger.info(f"Processamento concluído com sucesso!")
    logger.info(f"Parquet gerado em: {PARQUET_FINAL_PATH}")
    logger.info(f"Total de registros processados: {total_registros}")
else:
    logger.warning("Nenhum dado processado. Verifique os arquivos DBC.")