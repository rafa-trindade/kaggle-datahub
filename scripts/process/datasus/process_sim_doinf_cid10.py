"""SIM-DOINF - Declarações de Óbitos Infantis, CID-10 -- process."""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sim_doinf_cid10"
PASTA_BUCKET = "sim"
NOME_ARQUIVO_FINAL = "declaracoes_de_obito_infantis_cid10.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))