"""SIA/SUS - Sistema de Informações Ambulatoriais.

Sincroniza arquivos {PREFIXO}{UF}{AAMM}.dbc dos diretórios FTP.
A regra de extração previne colisão de prefixos (ex: AB vs ABO) validando o sufixo.
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

DIRETORIO_FTP_MODERNO = "/dissemin/publicos/SIASUS/200801_/Dados"
DIRETORIO_FTP_ANTIGO = "/dissemin/publicos/SIASUS/199407_200712/Dados"
PASTA_BUCKET = "sia"


def criar_regra(prefixo: str):
    """Retorna função de filtro para arquivos {prefixo}{UF}{AAMM}.dbc."""
    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        if not (nome.startswith(prefixo) and nome.endswith(".DBC")):
            return False
        resto = nome[len(prefixo):-4]
        # resto = UF (2 letras) + AAMM (4 dígitos) = 6 caracteres
        return len(resto) == 6 and resto[:2].isalpha() and resto[2:].isdigit()
    return regra


def executar_fetch(prefixo: str, output_subdir: str, diretorios_ftp: list[str] | None = None,
                   pasta_bucket: str | None = None):
    """Sincroniza um prefixo do SIA varrendo os diretórios FTP fornecidos.
    
    Permite injetar `pasta_bucket` para apontar o manifesto correto (ex: PA usa manifesto próprio).
    """
    if diretorios_ftp is None:
        diretorios_ftp = [DIRETORIO_FTP_MODERNO]
    if pasta_bucket is None:
        pasta_bucket = PASTA_BUCKET

    output_dir = str(LANDING_DIR / output_subdir)
    regra = criar_regra(prefixo)

    sucesso_geral = True
    houve_novidade = False
    for diretorio in diretorios_ftp:
        print(f"Sincronizando dados {prefixo} do diretório: {diretorio}")
        sucesso, novidade = sincronizar_ftp(diretorio, output_dir, regra, pasta_bucket=pasta_bucket)
        sucesso_geral = sucesso_geral and sucesso
        houve_novidade = houve_novidade or novidade

    if not sucesso_geral:
        exit(exit_codes.ERRO)
    elif not houve_novidade:
        print("[INFO] Nenhum arquivo novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)
