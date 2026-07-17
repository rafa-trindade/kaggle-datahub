"""
Módulo compartilhado do SINAN -- itera pela lista de agravos em
scripts.config.agravos_sinan, sincronizando cada um contra os dois
diretórios do FTP (FINAIS = anos fechados, PRELIM = ano corrente,
mesmo padrão dual-pasta já usado no SIM pra DO/DOEXT).

Arquivos: {PREFIXO}BR{AA}.dbc -- um por ano, Brasil inteiro (não
particionado por UF como SIM/SIH), então o volume por agravo é bem
menor.
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
        # resto = "BR" + ano de 2 dígitos = 4 caracteres (SINAN é Brasil
        # inteiro, sem UF -- diferente do SIH, que tem UF de 2 letras)
        return len(resto) == 4 and resto[:2] == "BR" and resto[2:].isdigit()
    return regra


def executar_fetch_um_agravo(prefixo: str, output_subdir: str) -> tuple[bool, bool]:
    """Sincroniza um agravo contra os dois diretórios. Devolve
    (sucesso_geral, houve_novidade)."""
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
    """Roda o fetch pra TODOS os agravos configurados, um de cada vez.
    Não para no primeiro erro -- um agravo com problema não deve
    travar os outros 57."""
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