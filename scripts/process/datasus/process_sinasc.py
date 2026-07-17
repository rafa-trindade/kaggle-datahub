"""
SINASC - Sistema de Informações sobre Nascidos Vivos -- process

Sem filtro -- materializa todas as Declarações de Nascido Vivo. Mesmo
padrão de mesclagem incremental usado no SIM (Declaração de Óbito e
Causas Externas).
"""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sinasc"
PASTA_BUCKET = "sinasc"
NOME_ARQUIVO_FINAL = "declaracoes_de_nascido_vivo.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))