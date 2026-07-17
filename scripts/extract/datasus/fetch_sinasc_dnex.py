"""
SINASC-DNEX - Declarações de Nascidos Vivos no Exterior

Fonte pequena e SEPARADA do SINASC principal (fetch_sinasc.py) --
brasileiros nascidos fora do país, registrados no sistema. Não pertence
a nenhuma UF (por isso não se mistura com o arquivo consolidado por
estado): confirmado como um recorte real e distinto pelo próprio nome
do arquivo, DNEX{AAAA}.dbc, na mesma pasta do SINASC "novo".
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

OUTPUT_DIR = str(LANDING_DIR / "dbc_sinasc_dnex")
PASTA_BUCKET = "sinasc"  # mesma pasta do SINASC principal -- só o nome do arquivo final é diferente


def regra_dnex(nome_arquivo: str) -> bool:
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