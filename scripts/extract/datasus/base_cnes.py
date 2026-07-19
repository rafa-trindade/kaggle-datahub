"""CNES via FTP: fontes organizadas por UF e competência.

CNES é um retrato da competência mais recente -- substitui anterior
completamente, não funde (diferente do SIM). Ver base_process_dbc.py.
"""
import re
import socket
from ftplib import FTP

from scripts.extract.datasus.base_ftp import FTPPasvFix, sincronizar_ftp, FTP_HOST
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes


def achar_competencia_mais_recente(diretorio_ftp: str, prefixo: str) -> str:
    """Retorna competência (AAMM) mais recente nos arquivos do diretório."""
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
    """Baixa arquivos da competência mais recente (todas as UFs)."""
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