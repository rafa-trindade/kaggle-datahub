"""SIA/SUS - AR (APAC de Radioterapia, 2008-atual) -- process."""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sia_ar"
PASTA_BUCKET = "sia"
NOME_ARQUIVO_FINAL = "apac_radioterapia.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))
