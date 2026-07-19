"""SINASC - Nascidos Vivos no Exterior (DNEX).

Separado do SINASC principal, não pertence a UF.
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

OUTPUT_DIR = str(LANDING_DIR / "dbc_sinasc_dnex")
PASTA_BUCKET = "sinasc"


def regra_dnex(nome_arquivo: str) -> bool:
    """Valida arquivo DNEX{AAAA}.dbc."""
    nome = nome_arquivo.upper()
    if not (nome.startswith("DNEX") and nome.endswith(".DBC")):
        return False
    ano_str = nome[4:-4]
    return ano_str.isdigit() and len(ano_str) == 4


FONTES_FTP = [
    {
        "diretorio": "/dissemin/publicos/SINASC/1996_/Dados/DNRES",
        "regra": regra_dnex,
        "tipo": "Nascidos no Exterior",
    }
]

if __name__ == "__main__":
    sucesso_geral = True
    houve_novidade = False

    for fonte in FONTES_FTP:
        print(f"Sincronizando dados {fonte['tipo']} do diretório: {fonte['diretorio']}")

        sucesso, novidade = sincronizar_ftp(fonte["diretorio"], OUTPUT_DIR, fonte["regra"], pasta_bucket=PASTA_BUCKET)
        sucesso_geral = sucesso_geral and sucesso
        houve_novidade = houve_novidade or novidade

    if not sucesso_geral:
        exit(exit_codes.ERRO)
    elif not houve_novidade:
        print("[INFO] Nenhum arquivo novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)