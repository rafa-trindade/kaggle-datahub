"""SIH/SUS - SP (Serviços Profissionais) -- process."""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sih_sp"
PASTA_BUCKET = "sih"
NOME_ARQUIVO_FINAL = "servicos_profissionais.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))