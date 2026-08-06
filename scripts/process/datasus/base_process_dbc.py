import os
import logging
import gc
from pathlib import Path
import datasus_dbc
from simpledbf import Dbf5 
from dbfread import DBF  
import duckdb
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from scripts.common.paths import BASE_DIR
from scripts.common import simpledbf_patch  # corrige bug de data zerada (00000000) na lib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DUCKDB_TEMP_DIR = Path(os.environ.get("DUCKDB_TEMP_DIR", str(BASE_DIR / "data" / ".duckdb_temp")))


def _fechar_dbf(dbf):
    """Fecha o handle do simpledbf com segurança para evitar lock de arquivo no Windows."""
    if dbf is None:
        return
    try:
        dbf.f.close()
    except (AttributeError, ValueError, OSError):
        pass


def _remover_seguro(caminho: str, tentativas: int = 5, espera: float = 0.2) -> bool:
    """os.remove com retries para lidar com lock momentâneo do Windows (WinError 32)."""
    import time
    for i in range(tentativas):
        try:
            if os.path.exists(caminho):
                os.remove(caminho)
            return True
        except PermissionError:
            time.sleep(espera * (i + 1))
        except OSError:
            break
    return not os.path.exists(caminho)


def _quarentena(dbc_dir: Path, caminho_dbc: str, arquivo: str):
    """Move um .dbc corrompido para _corrompidos/ evitando travamentos nas re-execuções."""
    try:
        quarentena_dir = dbc_dir / "_corrompidos"
        quarentena_dir.mkdir(exist_ok=True)
        destino = str(quarentena_dir / arquivo)
        if os.path.exists(destino):
            _remover_seguro(destino)
        shutil.move(caminho_dbc, destino)
        logger.warning(f"   ↳ {arquivo} movido para _corrompidos/ para inspeção.")
    except OSError as e:
        logger.warning(f"   ↳ Não foi possível mover {arquivo} para quarentena: {e}")


def _iter_chunks_simpledbf(caminho_dbf: str, chunksize: int):
    """Lê DBF via simpledbf em chunks (gera DataFrames string). Levanta AssertionError se o header for incompatível."""
    dbf = Dbf5(caminho_dbf, codec="latin1")
    try:
        for df_chunk in dbf.to_dataframe(chunksize=chunksize):
            yield df_chunk.astype(str)
    finally:
        _fechar_dbf(dbf)


def _iter_chunks_dbfread(caminho_dbf: str, chunksize: int):
    """Fallback: lê DBF via dbfread. Normaliza strings vazias para NaN para manter a paridade com o simpledbf."""
    import numpy as np

    def _para_string(df: pd.DataFrame) -> pd.DataFrame:
        """Converte DataFrame para string, mascarando strings vazias/espaços como NaN."""
        out = df.astype(str)
        for col in out.columns:
            vazio = out[col].str.strip().isin(["", "None", "nan", "NaT"])
            out[col] = out[col].mask(vazio, np.nan)
        return out

    tabela = DBF(caminho_dbf, encoding="latin1", ignore_missing_memofile=True,
                 char_decode_errors="replace")
    buffer = []
    for registro in tabela:
        buffer.append(registro)
        if len(buffer) >= chunksize:
            yield _para_string(pd.DataFrame(buffer))
            buffer.clear()
            gc.collect()
    if buffer:
        yield _para_string(pd.DataFrame(buffer))


def _iter_chunks_dbf(caminho_dbf: str, arquivo: str, chunksize: int = 250_000):
    """Tenta ler em chunks via simpledbf; cai para dbfread caso o arquivo tenha header rejeitado."""
    try:

        gerador = _iter_chunks_simpledbf(caminho_dbf, chunksize)
        primeiro = next(gerador, None)
    except AssertionError:
        gc.collect()
        logger.warning(f"   ↳ simpledbf recusou {arquivo}; usando dbfread (fallback).")
        yield from _iter_chunks_dbfread(caminho_dbf, chunksize)
        return

    # simpledbf aceitou: repassa o primeiro chunk e o restante
    if primeiro is not None:
        yield primeiro
        yield from gerador


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
    # # Fase 1: DBC -> DBF -> Parquets intermediários (em lotes)
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
        parquet_writer = None
        try:
            if os.path.exists(caminho_dbf): os.remove(caminho_dbf)
            datasus_dbc.decompress(caminho_dbc, caminho_dbf)

            for df_chunk in _iter_chunks_dbf(caminho_dbf, arquivo, chunksize=250_000):
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

            parquets_gerados.append(caminho_parquet_temp)
            _remover_seguro(caminho_dbf)
            _remover_seguro(caminho_dbc)

        except Exception as e:
            logger.error(f"❌ Falha ao converter {arquivo}: {type(e).__name__}: {e}")
            try:
                if parquet_writer:
                    parquet_writer.close()
            except (ValueError, OSError):
                pass
            _remover_seguro(caminho_dbf)
            _remover_seguro(caminho_parquet_temp)  # parquet meio-escrito, se houver
            if os.path.exists(caminho_dbc):
                _quarentena(dbc_dir, caminho_dbc, arquivo)
            continue

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
    """Processa novos .dbc e usa DuckDB para mesclá-los ao Parquet já publicado, atualizando revisões."""
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
    """Processa fontes estáticas (retrato), realizando substituição completa em vez de mesclagem."""
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