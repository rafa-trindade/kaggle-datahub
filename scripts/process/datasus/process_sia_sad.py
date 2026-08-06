"""SIA/SUS - SAD (Atenção Domiciliar, Nov/2012-atual) -- process."""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sia_sad"
PASTA_BUCKET = "sia"
NOME_ARQUIVO_FINAL = "atencao_domiciliar.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))
