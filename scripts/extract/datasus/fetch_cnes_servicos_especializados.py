"""CNES - Serviços Especializados por estabelecimento (competência mais recente)."""
from scripts.extract.datasus.base_cnes import executar_fetch_competencia_atual

DIRETORIO_FTP = "/dissemin/publicos/CNES/200508_/Dados/SR"

if __name__ == "__main__":
    executar_fetch_competencia_atual("SR", DIRETORIO_FTP, "dbc_cnes_servicos_especializados")
