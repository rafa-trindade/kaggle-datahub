"""
SIM - Causas Externas, CID-10 (1996-atual) -- process

Consolidados e Preliminares caem na mesma pasta da Landing (ver
scripts.extract.datasus.fetch_sim_causas_externas_cid10) e viram um
único Parquet final, mesmo padrão de mesclagem incremental usado em
declaracao_obito.
"""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sim_causas_externas" / "cid10"
PASTA_BUCKET = "sim"
NOME_ARQUIVO_FINAL = "declaracoes_de_obito_causas_externas_cid10.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))