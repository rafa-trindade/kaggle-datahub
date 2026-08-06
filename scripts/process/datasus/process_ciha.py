"""CIHA - Comunicação de Informação Hospitalar e Ambulatorial -- process."""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_ciha"
PASTA_BUCKET = "ciha"
NOME_ARQUIVO_FINAL = "comunicacao_internacao_hospitalar_ambulatorial.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))
