"""
SIMSUS Sistema de Informação sobre Mortalidade (SIM) 
Baixa arquivos .dbc do FTP do DATASUS (SIM - Causas Externas)
Dados Consolidados (desde 1996) e Dados Preliminares
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp

OUTPUT_DIR = "data/landing/dbc_sim_causas_externas/cid10"

def criar_regra_doext(ano_min: int = None, ano_max: int = None):
    """Gera a regra de validação do arquivo DOEXT com base em um intervalo de anos (4 dígitos)."""
    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        if not (nome.startswith("DOEXT") and nome.endswith(".DBC")):
            return False
        
        ano_str = nome[5:-4]
        if not ano_str.isdigit():
            return False
            
        ano_int = int(ano_str)
        
        if len(ano_str) == 2:
            ano_completo = 1900 + ano_int if ano_int >= 90 else 2000 + ano_int
        else:
            ano_completo = ano_int 
            
        if ano_min is not None and ano_max is not None:
            return ano_min <= ano_completo <= ano_max
            
        return True
        
    return regra

FONTES_FTP = [
    {
        "diretorio": "/dissemin/publicos/SIM/CID10/DOFET",
        "regra": criar_regra_doext(1996, 2026), 
        "tipo": "Consolidados"
    },
    {
        "diretorio": "/dissemin/publicos/SIM/PRELIM/DOFET",
        "regra": criar_regra_doext(),
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