"""
PO - Painel de Oncologia - desde 2013
Baixa arquivos .dbc do FTP do DATASUS (Painel Oncologia)
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp


FTP_DIR = "/dissemin/publicos/PAINEL_ONCOLOGIA/DADOS"
OUTPUT_DIR = "data/landing/dbc_datasus_po"

def regra_dbc_geral(nome_arquivo: str) -> bool:
    # Para o Painel de Oncologia, basta ser .DBC
    return nome_arquivo.upper().endswith(".DBC")

if __name__ == "__main__":
    updated = sincronizar_ftp(FTP_DIR, OUTPUT_DIR, regra_dbc_geral)
    exit(0 if updated else 1)