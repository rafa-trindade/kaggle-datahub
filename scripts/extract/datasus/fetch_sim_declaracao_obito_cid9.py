"""
SIMSUS Sistema de Informação sobre Mortalidade (SIM)
Baixa arquivos .dbc do FTP do DATASUS (SIM - Declaração de Óbito Geral)
Dados Consolidados (de 1979 a 1995)
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

OUTPUT_DIR = str(LANDING_DIR / "dbc_datasus_sim" / "cid9")

def criar_regra_dorbr(ano_min: int = None, ano_max: int = None):
    """Gera a regra de validação do arquivo DORBR resolvendo os 2 dígitos."""
    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        if not (nome.startswith("DORBR") and nome.endswith(".DBC")):
            return False
        
        ano_str = nome[5:-4]
        if not ano_str.isdigit():
            return False
            
        ano_int = int(ano_str)
        
        if len(ano_str) == 2:
            ano_completo = 1900 + ano_int if ano_int >= 79 else 2000 + ano_int
        else:
            ano_completo = ano_int
            
        if ano_min is not None and ano_max is not None:
            return ano_min <= ano_completo <= ano_max
        elif ano_min is not None:
            return ano_completo >= ano_min
            
        return True
        
    return regra

FONTES_FTP = [
    {
        "diretorio": "/dissemin/publicos/SIM/CID9/DORES",
        "regra": criar_regra_dorbr(1979, 1995),
        "tipo": "Consolidados"
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