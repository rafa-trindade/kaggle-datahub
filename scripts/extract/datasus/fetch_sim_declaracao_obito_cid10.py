"""
SIMSUS Sistema de Informação sobre Mortalidade (SIM)
Baixa arquivos .dbc do FTP do DATASUS (SIM - Declaração de Óbito)
Dados Consolidados (desde 1996) e Preliminares
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp

OUTPUT_DIR = "data/landing/dbc_datasus_sim/cid10"

def criar_regra_dobr(ano_minimo: int):
    """Factory que gera a função de regra baseada no ano de corte."""
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

if __name__ == "__main__":
    houve_atualizacao = False
    
    for fonte in FONTES_FTP:
        print(f"Sincronizando dados {fonte['tipo']} do diretório: {fonte['diretorio']}")
        
        updated = sincronizar_ftp(fonte["diretorio"], OUTPUT_DIR, fonte["regra"])
        if updated:
            houve_atualizacao = True

    exit(0 if houve_atualizacao else 1)