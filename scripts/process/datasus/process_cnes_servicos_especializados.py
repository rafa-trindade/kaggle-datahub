"""CNES - Serviços Especializados -- process."""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_substituicao_completa

DBC_DIR = LANDING_DIR / "dbc_cnes_servicos_especializados"
PASTA_BUCKET = "cnes"
NOME_ARQUIVO_FINAL = "servicos_especializados.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_substituicao_completa(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL, chave_manifesto_prefixo="SR"))
