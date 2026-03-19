"""
SIMSUS Sistema de Informação sobre Mortalidade (SIM)
Baixa arquivos .dbc do FTP do DATASUS (SIM - Declaração de Óbito Geral)
Dados Consolidados (de 1979 a 1995)
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp

OUTPUT_DIR = "data/landing/dbc_datasus_sim/cid9"

def criar_regra_dorbr(ano_min: int = None, ano_max: int = None):
    """Gera a regra de validação do arquivo DORBR resolvendo os 2 dígitos."""
    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        # Mudança 1: Procurando por DORBR em vez de DOBR
        if not (nome.startswith("DORBR") and nome.endswith(".DBC")):
            return False
        
        # Mudança 2: Como DORBR tem 5 letras, o ano começa no índice 5
        ano_str = nome[5:-4]
        if not ano_str.isdigit():
            return False
            
        ano_int = int(ano_str)
        
        # Resolve o Bug do Milênio: de 79 pra cima é 1900.
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

if __name__ == "__main__":
    houve_atualizacao = False
    
    for fonte in FONTES_FTP:
        print(f"Sincronizando dados {fonte['tipo']} do diretório: {fonte['diretorio']}")
        
        updated = sincronizar_ftp(fonte["diretorio"], OUTPUT_DIR, fonte["regra"])
        if updated:
            houve_atualizacao = True

    exit(0 if houve_atualizacao else 1)