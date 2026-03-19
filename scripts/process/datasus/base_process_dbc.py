import os
import logging
import gc
from pathlib import Path
import datasus_dbc
from simpledbf import Dbf5 
import duckdb
import shutil
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def processar_diretorio_dbc(dbc_dir: Path, csv_final_path: Path):
    arquivos_dbc = [f for f in os.listdir(dbc_dir) if f.lower().endswith(".dbc")]
    arquivos_dbc.sort()

    if not arquivos_dbc:
        logger.warning(f"Nenhum arquivo .dbc encontrado em {dbc_dir}.")
        return

    logger.info(f"Arquivos encontrados: {len(arquivos_dbc)}")
    
    temp_dir = dbc_dir / "temp_parquets"
    temp_dir.mkdir(exist_ok=True)

    # ---------------------------------------------------------
    # FASE 1: DBC -> DBF -> Mini Parquet
    # ---------------------------------------------------------
    logger.info("Fase 1: Convertendo DBCs para Parquets intermediários (em lotes)...")
    parquets_gerados = []
    
    for idx, arquivo in enumerate(arquivos_dbc, 1):
        caminho_dbc = str(dbc_dir / arquivo)
        caminho_dbf = caminho_dbc.replace(".DBC", ".DBF").replace(".dbc", ".dbf")
        caminho_parquet_temp = str(temp_dir / arquivo.replace(".dbc", ".parquet").replace(".DBC", ".parquet"))
        
        if os.path.exists(caminho_parquet_temp):
            parquets_gerados.append(caminho_parquet_temp)
            logger.info(f"[{idx}/{len(arquivos_dbc)}] [SKIP] {arquivo} (Parquet temp já existe)")
            continue
            
        logger.info(f"[{idx}/{len(arquivos_dbc)}] Convertendo {arquivo}...")
        try:
            if os.path.exists(caminho_dbf): os.remove(caminho_dbf)
            datasus_dbc.decompress(caminho_dbc, caminho_dbf)
            
            dbf = Dbf5(caminho_dbf, codec='latin1')
            
            parquet_writer = None
            
            for df_chunk in dbf.to_dataframe(chunksize=250_000):
                df_chunk = df_chunk.astype(str)
                table = pa.Table.from_pandas(df_chunk)
                
                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(caminho_parquet_temp, table.schema)
                
                parquet_writer.write_table(table)
                
                del df_chunk
                del table
                gc.collect()
                
            if parquet_writer:
                parquet_writer.close()
            # --------------------------------------------------------
            
            parquets_gerados.append(caminho_parquet_temp)
            os.remove(caminho_dbf)
            
        except Exception as e:
            logger.error(f"❌ Falha ao converter {arquivo}: {e}")
            if os.path.exists(caminho_dbf): os.remove(caminho_dbf)

    if not parquets_gerados:
        logger.error("Nenhum arquivo convertido com sucesso. Abortando.")
        return

    # ---------------------------------------------------------
    # FASE 2: DuckDB Union e Consolidate para CSV
    # ---------------------------------------------------------
    logger.info("Fase 2: Consolidando todos os Parquets para CSV usando DuckDB...")
    
    padrao_leitura = str(temp_dir / "*.parquet")
    
    query = f"""
        COPY (
            SELECT * FROM read_parquet('{padrao_leitura}', union_by_name=True)
        ) TO '{str(csv_final_path)}' (HEADER, DELIMITER ',');
    """
    
    try:
        con = duckdb.connect(database=':memory:', config={
            'temp_directory': '/mnt/nvme/duckdb_temp/',
            'memory_limit': '4GB' 
        })
        con.execute("PRAGMA threads=4;") 
        
        con.execute(query)
        
        contagem = con.execute(f"SELECT COUNT(*) FROM read_parquet('{padrao_leitura}')").fetchone()[0]
        logger.info(f"Processamento concluído! {contagem} registros consolidados em {csv_final_path.name}")
        
    except Exception as e:
        logger.error(f"❌ Falha no DuckDB durante a consolidação: {e}")
    finally:
        con.close()

    shutil.rmtree(temp_dir)