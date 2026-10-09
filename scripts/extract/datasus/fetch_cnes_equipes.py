"""CNES - Equipes de saúde por estabelecimento (competência mais recente)."""
from scripts.extract.datasus.base_cnes import executar_fetch_competencia_atual

DIRETORIO_FTP = "/dissemin/publicos/CNES/200508_/Dados/EP"

if __name__ == "__main__":
    executar_fetch_competencia_atual("EP", DIRETORIO_FTP, "dbc_cnes_equipes")
