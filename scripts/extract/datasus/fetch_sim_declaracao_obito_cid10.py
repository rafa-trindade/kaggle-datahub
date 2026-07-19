"""SIM - Declaração de Óbito Geral, CID-10 (1996-atual, consolidados + preliminares)."""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

OUTPUT_DIR = str(LANDING_DIR / "dbc_datasus_sim" / "cid10")

def criar_regra_dobr(ano_minimo: int):
    """Valida arquivo DOBR a partir do ano mínimo."""
    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        if not (nome.startswith("DOBR") and nome.endswith(".DBC")):
            return False
        
        ano_str = nome[4:8]
        if not ano_str.isdigit():
            return False
            
        return int(ano_str) >= ano_minimo
    return regra

FONTES_FTP = [
    {
        "diretorio": "/dissemin/publicos/SIM/CID10/DORES",
        "regra": criar_regra_dobr(1996),
        "tipo": "Consolidados"
    },
    {
        "diretorio": "/dissemin/publicos/SIM/PRELIM/DORES",
        "regra": criar_regra_dobr(2016),
        "tipo": "Preliminares"
    }
]

PASTA_BUCKET = "sim"

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