"""SIA/SUS - ATD (APAC de Tratamento Dialítico, Jun/2014-atual) -- process."""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sia_atd"
PASTA_BUCKET = "sia"
NOME_ARQUIVO_FINAL = "apac_tratamento_dialitico.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))
