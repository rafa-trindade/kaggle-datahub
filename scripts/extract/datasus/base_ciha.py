"""CIHA -- Comunicação de Informação Hospitalar e Ambulatorial.

Série histórica: CIHA{UF}{AAMM}.dbc no diretório 201101_ (Jan/2011 em
diante). Sucede o CIH (que ia de 2008 a 2010). Mesma infra do SIH --
merge incremental.
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

DIRETORIO_FTP = "/dissemin/publicos/CIHA/201101_/Dados"
PASTA_BUCKET = "ciha"


def criar_regra(prefixo: str):
    """Regra de filtro: {prefixo}{UF}{AAMM}.dbc"""
    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        if not (nome.startswith(prefixo) and nome.endswith(".DBC")):
            return False
        resto = nome[len(prefixo):-4]
        # resto = UF (2 letras) + AAMM (4 dígitos) = 6 caracteres
        return len(resto) == 6 and resto[:2].isalpha() and resto[2:].isdigit()
    return regra


def executar_fetch(prefixo: str, output_subdir: str):
    output_dir = str(LANDING_DIR / output_subdir)
    regra = criar_regra(prefixo)

    print(f"Sincronizando dados {prefixo} do diretório: {DIRETORIO_FTP}")
    sucesso, novidade = sincronizar_ftp(DIRETORIO_FTP, output_dir, regra, pasta_bucket=PASTA_BUCKET)

    if not sucesso:
        exit(exit_codes.ERRO)
    elif not novidade:
        print("[INFO] Nenhum arquivo novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)
