"""
SIM - Declaração de Óbito, CID-9 (1979-1995) -- process

Diferente do onco-360-foundation (que filtra só óbitos por câncer),
aqui materializa TODOS os óbitos, sem filtro de causa -- é o hub bruto.

Processa só os .dbc novos/alterados presentes na Landing (o extract já
pulou os que não mudaram, via manifesto -- ver
scripts.extract.datasus.fetch_sim_declaracao_obito_cid9), mescla com o
que já está publicado no bucket, e republica o resultado consolidado.
"""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_datasus_sim" / "cid9"
PASTA_BUCKET = "sim"
NOME_ARQUIVO_FINAL = "declaracoes_de_obito_cid9.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))