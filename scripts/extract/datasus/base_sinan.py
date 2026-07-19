"""SINAN -- sincroniza agravos contra FINAIS (histórico) e PRELIM (corrente).

Arquivos: {PREFIXO}BR{AA}.dbc (Brasil inteiro, sem UF).
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

DIRETORIOS_FTP = [
    "/dissemin/publicos/SINAN/DADOS/FINAIS",
    "/dissemin/publicos/SINAN/DADOS/PRELIM",
]
PASTA_BUCKET = "sinan"


def criar_regra(prefixo: str):
    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        if not (nome.startswith(prefixo) and nome.endswith(".DBC")):
            return False
        resto = nome[len(prefixo):-4]
        # resto = "BR" + ano 2 dígitos (Brasil inteiro, sem UF)
        return len(resto) == 4 and resto[:2] == "BR" and resto[2:].isdigit()
    return regra


def executar_fetch_um_agravo(prefixo: str, output_subdir: str) -> tuple[bool, bool]:
    """Sincroniza um agravo contra ambos diretórios."""
    output_dir = str(LANDING_DIR / output_subdir)
    regra = criar_regra(prefixo)

    sucesso_geral = True
    houve_novidade = False
    for diretorio in DIRETORIOS_FTP:
        sucesso, novidade = sincronizar_ftp(diretorio, output_dir, regra, pasta_bucket=PASTA_BUCKET)
        sucesso_geral = sucesso_geral and sucesso
        houve_novidade = houve_novidade or novidade

    return sucesso_geral, houve_novidade


def executar_fetch_todos_agravos(agravos: list[tuple[str, str]]):
    """Roda fetch para todos agravos. Não para no primeiro erro."""
    algum_sucesso = False
    algum_erro = False
    algum_novo = False

    for prefixo, nome_arquivo in agravos:
        print(f"=== {nome_arquivo} ({prefixo}) ===")
        output_subdir = f"dbc_sinan_{nome_arquivo}"
        try:
            sucesso, novidade = executar_fetch_um_agravo(prefixo, output_subdir)
        except Exception as e:
            print(f"[ERRO] Falha inesperada em {prefixo}: {e}")
            algum_erro = True
            continue

        if sucesso:
            algum_sucesso = True
            if novidade:
                algum_novo = True
        else:
            algum_erro = True

    if algum_erro and not algum_sucesso:
        exit(exit_codes.ERRO)
    elif not algum_novo:
        print("\n[INFO] Nenhum agravo teve arquivo novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)