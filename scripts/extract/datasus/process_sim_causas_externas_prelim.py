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

DBC_DIR = os.path.join(base_dir, "data", "utils", "dbc_sim_causas_externas", "prelim")
PARQUET_FINAL_PATH = os.path.join(base_dir, "data", "raw", "raw_sim_causas_externas_prelim.parquet")
CSV_TEMP_PATH = os.path.join(base_dir, "data", "raw", "raw_sim_causas_externas_prelim_temp.csv")

os.makedirs(os.path.join(base_dir, "data", "raw"), exist_ok=True)
# -----------------------------
# Variáveis de controle
# -----------------------------
batch_size = 50_000
total_registros = 0
csv_header_written = False
colunas_finais_dinamicas = [] 

# -----------------------------
# Seleção dos arquivos DBC
# -----------------------------
arquivos_dbc = [f for f in os.listdir(DBC_DIR) if f.lower().endswith(".dbc")]
arquivos_dbc.sort()

if not arquivos_dbc:
    logger.warning(f"Nenhum arquivo .dbc encontrado em {DBC_DIR}.")
    
logger.info(f"Arquivos encontrados: {len(arquivos_dbc)}")

# -----------------------------
# Processamento dos arquivos
# -----------------------------
try:
    with open(CSV_TEMP_PATH, 'a', newline='', encoding="utf-8") as f_csv:
        csv_writer = csv.writer(f_csv, delimiter=';')

        for idx, arquivo in enumerate(arquivos_dbc, 1):
            
            caminho_dbc = os.path.join(DBC_DIR, arquivo)
            caminho_dbf = caminho_dbc.replace(".DBC", ".DBF").replace(".dbc", ".dbf")

            logger.info(f"[{idx}/{len(arquivos_dbc)}] Processando {arquivo}...")

            if os.path.exists(caminho_dbf):
                os.remove(caminho_dbf)
                logger.warning(f"DBF pré-existente removido: {caminho_dbf}")

           
            try:
                logger.info(f"Descompactando {arquivo} (tamanho: {round(os.path.getsize(caminho_dbc) / (1024*1024), 2)} MB)...")
                
                start_time = time.time()
                datasus_dbc.decompress(caminho_dbc, caminho_dbf) 
                end_time = time.time()

                logger.info(f"Descompactação concluída em {round(end_time - start_time, 2)} segundos.")
                
                if not os.path.exists(caminho_dbf):
                    raise FileNotFoundError("Descompactação falhou. Arquivo DBF não encontrado.")

            except Exception as e:
                logger.error(f"❌ Falha CRÍTICA ao descompactar {arquivo}. Verifique se o arquivo está lá: {e}")
                continue

            tabela = DBF(caminho_dbf, encoding="latin1") 
            batch = []
            count = 0
            
            if idx == 1:
                colunas_finais_dinamicas = tabela.field_names
                logger.info(f"Colunas detectadas: {colunas_finais_dinamicas}")
                if not csv_header_written:
                    csv_writer.writerow(colunas_finais_dinamicas)
                    csv_header_written = True

            for registro in tabela:
                count += 1
                total_registros += 1
                registro_formatado = [str(registro.get(k, "")).strip() for k in colunas_finais_dinamicas]
                batch.append(registro_formatado)

                if len(batch) >= batch_size:
                    csv_writer.writerows(batch)
                    batch.clear()

            if batch:
                csv_writer.writerows(batch)
                batch.clear()

            logger.info(f"{arquivo}: {count} registros processados.")

            if os.path.exists(caminho_dbf):
                os.remove(caminho_dbf)
                logger.info(f"DBF temporário removido: {caminho_dbf}")
                
except Exception as main_e:
    logger.error(f"Ocorreu um erro principal durante o processamento: {main_e}")


# -----------------------------
# Conversão final e remoção CSV
# -----------------------------
if total_registros > 0 and os.path.exists(CSV_TEMP_PATH):
    logger.info("Convertendo CSV para Parquet...")
    try:
        df_final = pd.read_csv(CSV_TEMP_PATH, sep=";", dtype=str, encoding="utf-8")
        table = pa.Table.from_pandas(df_final, preserve_index=False)
        pq.write_table(table, PARQUET_FINAL_PATH)

        logger.info(f"Parquet gerado em: {PARQUET_FINAL_PATH}")
        logger.info(f"Registros processados: {total_registros}")

        os.remove(CSV_TEMP_PATH)
        logger.info(f"CSV temporário removido: {CSV_TEMP_PATH}")
    except Exception as e:
        logger.error(f"Falha na conversão final para Parquet: {e}")

else:
    logger.warning("Nenhum dado processado. Nenhum parquet criado.")