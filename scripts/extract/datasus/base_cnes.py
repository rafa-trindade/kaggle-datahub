"""
Módulo compartilhado pras fontes do CNES que vêm via FTP, organizadas
por UF e competência (Habilitações, Leitos) -- mesmo padrão usado no
onco-360-foundation, mas SEM filtro de especialidade (todas as áreas,
não só oncologia).

Diferença semântica importante em relação ao SIM: CNES não é série
histórica que acumula ano a ano -- é um RETRATO da competência (mês)
mais recente disponível. Quando uma competência nova aparece, ela
SUBSTITUI a anterior por completo (não funde) -- ver
processar_fonte_ftp_substituicao_completa em base_process_dbc.py.
"""
import re
import socket
from ftplib import FTP

from scripts.extract.datasus.base_ftp import FTPPasvFix, sincronizar_ftp, FTP_HOST
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes


def achar_competencia_mais_recente(diretorio_ftp: str, prefixo: str) -> str:
    """Lista o diretório e acha a competência (AAMM, 4 dígitos) mais
    recente entre os nomes de arquivo -- ex: HBAC2605.dbc -> '2605'."""
    ip_v4 = socket.gethostbyname(FTP_HOST)
    with FTPPasvFix() as ftp:
        ftp.connect(ip_v4, 21, timeout=30)
        ftp.login()
        ftp.set_pasv(True)
        ftp.cwd(diretorio_ftp)
        arquivos = ftp.nlst()

    competencias = set()
    padrao = re.compile(rf"^{prefixo}[A-Z]{{2}}(\d{{4}})\.dbc$", re.IGNORECASE)
    for nome in arquivos:
        m = padrao.match(nome)
        if m:
            competencias.add(m.group(1))

    if not competencias:
        raise ValueError(f"Nenhuma competência encontrada em {diretorio_ftp} com prefixo {prefixo}")

    return max(competencias)


def executar_fetch_competencia_atual(prefixo: str, diretorio_ftp: str, output_subdir: str):
    """Acha a competência mais recente, baixa só os arquivos dela
    (todas as UFs, sem filtro de especialidade)."""
    competencia = achar_competencia_mais_recente(diretorio_ftp, prefixo)
    print(f"Competência mais recente encontrada: {competencia}")

    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        return nome.startswith(prefixo) and nome.endswith(f"{competencia}.DBC")

    output_dir = str(LANDING_DIR / output_subdir)
    sucesso, novidade = sincronizar_ftp(diretorio_ftp, output_dir, regra, pasta_bucket="cnes")

    if not sucesso:
        exit(exit_codes.ERRO)
    elif not novidade:
        print("[INFO] Nenhum arquivo novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)