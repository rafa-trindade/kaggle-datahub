"""
SIM - Declaração de Óbito, CID-10 (1996-atual) -- process

Diferente do onco-360-foundation (que filtra só óbitos por câncer),
aqui materializa TODOS os óbitos, sem filtro de causa -- é o hub bruto.

Consolidados e Preliminares caem na mesma pasta da Landing (ver
scripts.extract.datasus.fetch_sim_declaracao_obito_cid10) e viram um
único Parquet final -- o preliminar é só o ano corrente ainda não
fechado pelo DATASUS, natural que conviva com o resto da série no
mesmo arquivo (revisado/substituído automaticamente quando o DATASUS
consolida esse ano, via o mecanismo de merge incremental por
_ARQUIVO_ORIGEM).
"""
from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental

DBC_DIR = LANDING_DIR / "dbc_datasus_sim" / "cid10"
PASTA_BUCKET = "sim"
NOME_ARQUIVO_FINAL = "declaracoes_de_obito_cid10.parquet"

if __name__ == "__main__":
    exit(processar_fonte_ftp_incremental(DBC_DIR, PASTA_BUCKET, NOME_ARQUIVO_FINAL))