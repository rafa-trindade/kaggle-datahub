"""SIM - Categorias na pasta DOFET (óbitos fetais, infantis, maternos, exterior).

Nota: todos usam ano em 2 dígitos (diferente de DORES com 4 dígitos).
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

PASTA_BUCKET = "sim"


def criar_regra(prefixo: str):
    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        if not (nome.startswith(prefixo) and nome.endswith(".DBC")):
            return False
        ano_str = nome[len(prefixo):-4]
        return ano_str.isdigit() and len(ano_str) == 2
    return regra


def executar_fetch(prefixo: str, diretorio_ftp: str, output_subdir: str):
    """Entrada comum para um diretório."""
    executar_fetch_multiplos(prefixo, [diretorio_ftp], output_subdir)


def executar_fetch_multiplos(prefixo: str, diretorios_ftp: list[str], output_subdir: str):
    """Múltiplos diretórios (ex: consolidado + preliminar) na mesma subpasta."""
    output_dir = str(LANDING_DIR / output_subdir)
    regra = criar_regra(prefixo)

    sucesso_geral = True
    houve_novidade = False

    for diretorio in diretorios_ftp:
        print(f"Sincronizando dados {prefixo} do diretório: {diretorio}")
        sucesso, novidade = sincronizar_ftp(diretorio, output_dir, regra, pasta_bucket=PASTA_BUCKET)
        sucesso_geral = sucesso_geral and sucesso
        houve_novidade = houve_novidade or novidade

    if not sucesso_geral:
        exit(exit_codes.ERRO)
    elif not houve_novidade:
        print("[INFO] Nenhum arquivo novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)