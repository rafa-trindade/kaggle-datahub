"""
SIM - Causas Externas, CID-9 (1979-1995) -- process

Sistema separado do DATASUS (DOFET), não é um filtro sobre a
Declaração de Óbito geral -- óbitos por causas externas (acidentes,
violência) têm seu próprio conjunto de arquivos na origem.
"""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sim_causas_externas" / "cid9"
PASTA_BUCKET = "sim"
NOME_ARQUIVO_FINAL = "declaracoes_de_obito_causas_externas_cid9.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))