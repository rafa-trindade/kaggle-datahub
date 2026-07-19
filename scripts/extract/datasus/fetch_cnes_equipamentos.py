"""CNES - Equipamentos por estabelecimento (competência mais recente)."""
from scripts.extract.datasus.base_cnes import executar_fetch_competencia_atual

DIRETORIO_FTP = "/dissemin/publicos/CNES/200508_/Dados/EQ"

if __name__ == "__main__":
    executar_fetch_competencia_atual("EQ", DIRETORIO_FTP, "dbc_cnes_equipamentos")