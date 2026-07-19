"""CNES - Habilitações por estabelecimento (competência mais recente)."""
from scripts.extract.datasus.base_cnes import executar_fetch_competencia_atual

DIRETORIO_FTP = "/dissemin/publicos/CNES/200508_/Dados/HB"

if __name__ == "__main__":
    executar_fetch_competencia_atual("HB", DIRETORIO_FTP, "dbc_cnes_habilitacoes")