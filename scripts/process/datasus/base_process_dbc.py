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

from scripts.common.paths import BASE_DIR
from scripts.common import simpledbf_patch  # corrige bug de data zerada (00000000) na lib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Temp dir DuckDB (configurável via env, fallback no projeto)
DUCKDB_TEMP_DIR = Path(os.environ.get("DUCKDB_TEMP_DIR", str(BASE_DIR / "data" / ".duckdb_temp")))


def listar_dbc_deduplicados(dbc_dir: Path) -> list[str]:
    """Lista .dbc do diretório, removendo duplicatas por maiúscula/minúscula (ex: X.DBC e X.dbc)."""
    vistos = set()
    arquivos = []
    for f in sorted(os.listdir(dbc_dir)):
        if not f.lower().endswith(".dbc"):
            continue
        chave = f.upper()
        if chave in vistos:
            continue
        vistos.add(chave)
        arquivos.append(f)
    return arquivos


def processar_diretorio_dbc(dbc_dir: Path, parquet_final_path: Path) -> bool:
    """Converte .dbc para Parquet consolidado. Retorna True se sucesso."""
    arquivos_dbc = listar_dbc_deduplicados(dbc_dir)

    if not arquivos_dbc:
        logger.warning(f"Nenhum arquivo .dbc encontrado em {dbc_dir}.")
        return False

    logger.info(f"Arquivos encontrados: {len(arquivos_dbc)}")
    
    temp_dir = dbc_dir / "temp_parquets"
    temp_dir.mkdir(exist_ok=True)

    # ---------------------------------------------------------
    # # Fase 1: DBC -> DBF -> Parquets intermediários
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
                df_chunk["_ARQUIVO_ORIGEM"] = arquivo   # rastrear origem para merge incremental
                table = pa.Table.from_pandas(df_chunk)
                
                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(caminho_parquet_temp, table.schema)
                
                parquet_writer.write_table(table)
                
                del df_chunk
                del table
                gc.collect()
                
            if parquet_writer:
                parquet_writer.close()

            dbf.f.close()  # simpledbf não fecha handle sozinho (impede delete no Windows)
            
            parquets_gerados.append(caminho_parquet_temp)
            os.remove(caminho_dbf)
            os.remove(caminho_dbc) 
            
        except Exception as e:
            logger.error(f"❌ Falha ao converter {arquivo}: {e}")
            try:
                dbf.f.close()
            except (NameError, AttributeError, ValueError):
                pass  # dbf pode não existir ou handle já estar fechado
            if os.path.exists(caminho_dbf): os.remove(caminho_dbf)

    if not parquets_gerados:
        logger.error("Nenhum arquivo convertido com sucesso. Abortando.")
        return False

    # ---------------------------------------------------------
    # Fase 2: Consolidar Parquets via DuckDB
    # ---------------------------------------------------------
    logger.info("Fase 2: Consolidando todos os Parquets intermediários num único Parquet final (DuckDB)...")
    
    padrao_leitura = str(temp_dir / "*.parquet")

    parquet_final_path.parent.mkdir(parents=True, exist_ok=True)

    query = f"""
        COPY (
            SELECT * FROM read_parquet('{padrao_leitura}', union_by_name=True)
        ) TO '{str(parquet_final_path)}' (FORMAT PARQUET, ROW_GROUP_SIZE 250000);
    """

    DUCKDB_TEMP_DIR.mkdir(parents=True, exist_ok=True)

    con = None
    sucesso = False
    try:
        con = duckdb.connect(database=':memory:', config={
            'temp_directory': str(DUCKDB_TEMP_DIR),
            'memory_limit': '4GB' 
        })
        con.execute("PRAGMA threads=4;") 
        
        con.execute(query)
        
        contagem = con.execute(f"SELECT COUNT(*) FROM read_parquet('{padrao_leitura}')").fetchone()[0]
        logger.info(f"Processamento concluído! {contagem} registros consolidados em {parquet_final_path.name}")
        sucesso = True
        
    except Exception as e:
        logger.error(f"❌ Falha no DuckDB durante a consolidação: {e}")
    finally:
        if con is not None:
            con.close()

    shutil.rmtree(temp_dir)
    return sucesso


def processar_e_publicar_incremental(dbc_dir: Path, pasta_bucket: str, nome_arquivo_final: str) -> bool:
    """Processa .dbc novo/alterado e mescla com Parquet já publicado.

    Evita duplicação quando DATASUS revisa dados; preserva histórico.
    Usa DuckDB para não sobrecarregar memória.
    """
    from scripts.common.bucket_sync import get_s3_client, upload_and_cleanup
    from scripts.common import env

    s3_key = f"{pasta_bucket}/{nome_arquivo_final}"

    arquivos_dbc = listar_dbc_deduplicados(dbc_dir)
    if not arquivos_dbc:
        logger.info(f"Nenhum .dbc novo/alterado em {dbc_dir} -- nada a processar.")
        return False

    nomes_novos = set(arquivos_dbc)

    parquet_novos_temp = dbc_dir / "_novos_temp.parquet"
    if not processar_diretorio_dbc(dbc_dir, parquet_novos_temp):
        return False

    caminho_existente_temp = dbc_dir / "_existente_temp.parquet"
    tem_existente = False
    s3 = get_s3_client()
    try:
        s3.download_file(env.MINIO_BUCKET, s3_key, str(caminho_existente_temp))
        tem_existente = True
        logger.info(f"Parquet já publicado encontrado em {s3_key} -- mesclando com os arquivos novos.")
    except Exception:
        logger.info(f"Nada publicado ainda em {s3_key} -- esta é a primeira publicação.")

    caminho_final_temp = dbc_dir / nome_arquivo_final

    DUCKDB_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=':memory:', config={
        'temp_directory': str(DUCKDB_TEMP_DIR),
        'memory_limit': '4GB',
    })
    con.execute("PRAGMA threads=4;")

    try:
        if tem_existente:
            lista_nomes = ", ".join(f"'{n}'" for n in nomes_novos)
            query = f"""
                COPY (
                    SELECT * FROM read_parquet('{caminho_existente_temp}')
                    WHERE _ARQUIVO_ORIGEM NOT IN ({lista_nomes})
                    UNION ALL BY NAME
                    SELECT * FROM read_parquet('{parquet_novos_temp}')
                ) TO '{caminho_final_temp}' (FORMAT PARQUET, ROW_GROUP_SIZE 250000);
            """
        else:
            query = f"""
                COPY (SELECT * FROM read_parquet('{parquet_novos_temp}'))
                TO '{caminho_final_temp}' (FORMAT PARQUET, ROW_GROUP_SIZE 250000);
            """
        con.execute(query)
        contagem = con.execute(f"SELECT COUNT(*) FROM read_parquet('{caminho_final_temp}')").fetchone()[0]
        logger.info(f"✔ {contagem} registros no Parquet final mesclado ({nome_arquivo_final}).")
    except Exception as e:
        logger.error(f"❌ Falha ao mesclar/publicar: {e}")
        return False
    finally:
        con.close()

    parquet_novos_temp.unlink(missing_ok=True)
    if tem_existente:
        caminho_existente_temp.unlink(missing_ok=True)

    return upload_and_cleanup(caminho_final_temp, s3_key)


def processar_fonte_ftp_incremental(dbc_dir: Path, pasta_bucket: str, nome_arquivo_final: str) -> int:
    """Orquestra processo + manifesto. Retorna exit code (SEM_NOVIDADE/ERRO/SUCESSO)."""
    from scripts.common import exit_codes
    from scripts.common.bucket_sync import carregar_manifesto, salvar_manifesto

    if not dbc_dir.exists():
        logger.info(f"{dbc_dir} não existe -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    arquivos_presentes = {
        f: (dbc_dir / f).stat().st_size
        for f in listar_dbc_deduplicados(dbc_dir)
    }

    if not arquivos_presentes:
        logger.info("Nenhum .dbc novo/alterado -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    sucesso = processar_e_publicar_incremental(dbc_dir, pasta_bucket, nome_arquivo_final)
    if not sucesso:
        return exit_codes.ERRO

    manifesto = carregar_manifesto(pasta_bucket)
    manifesto = {k.upper(): v for k, v in manifesto.items()}
    manifesto.update({k.upper(): v for k, v in arquivos_presentes.items()})
    salvar_manifesto(pasta_bucket, manifesto)

    return exit_codes.SUCESSO


def processar_fonte_ftp_substituicao_completa(dbc_dir: Path, pasta_bucket: str, nome_arquivo_final: str,
                                                chave_manifesto_prefixo: str) -> int:
    """Processa fontes que são retrato (não série histórica).

    Substitui completo, não mescla. chave_manifesto_prefixo limpa
    entradas antigas desta fonte no manifesto compartilhado.
    """
    from scripts.common import exit_codes
    from scripts.common.bucket_sync import carregar_manifesto, salvar_manifesto, upload_and_cleanup

    if not dbc_dir.exists():
        logger.info(f"{dbc_dir} não existe -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    arquivos_presentes = {
        f: (dbc_dir / f).stat().st_size
        for f in listar_dbc_deduplicados(dbc_dir)
    }

    if not arquivos_presentes:
        logger.info("Nenhum .dbc novo/alterado -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    caminho_final_temp = dbc_dir / nome_arquivo_final
    if not processar_diretorio_dbc(dbc_dir, caminho_final_temp):
        return exit_codes.ERRO

    s3_key = f"{pasta_bucket}/{nome_arquivo_final}"
    sucesso = upload_and_cleanup(caminho_final_temp, s3_key)
    if not sucesso:
        return exit_codes.ERRO

    manifesto = carregar_manifesto(pasta_bucket)
    manifesto = {k.upper(): v for k, v in manifesto.items() if not k.upper().startswith(chave_manifesto_prefixo.upper())}
    manifesto.update({k.upper(): v for k, v in arquivos_presentes.items()})
    salvar_manifesto(pasta_bucket, manifesto)

    return exit_codes.SUCESSO