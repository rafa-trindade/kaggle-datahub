"""SINASC - Nascidos Vivos -- process."""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sinasc"
PASTA_BUCKET = "sinasc"
NOME_ARQUIVO_FINAL = "declaracoes_de_nascido_vivo.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))