"""
SINASC-DNEX - Declarações de Nascidos Vivos no Exterior -- process

Fonte pequena e separada do SINASC principal.
"""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_sinasc_dnex"
PASTA_BUCKET = "sinasc"  # mesma pasta do SINASC principal -- só o nome do arquivo final é diferente
NOME_ARQUIVO_FINAL = "declaracoes_de_nascido_vivo_exterior.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))