"""SIA/SUS - ACF (APAC de Confecção de Fístula Arteriovenosa, Jun/2014-atual) -- process."""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sia_acf"
PASTA_BUCKET = "sia"
NOME_ARQUIVO_FINAL = "apac_confeccao_fistula.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))
