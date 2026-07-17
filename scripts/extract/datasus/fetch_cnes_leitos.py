"""CNES - Leitos -- todos os leitos, sem filtro de especialidade.
Competência mais recente disponível (retrato, não série histórica)."""
from scripts.extract.datasus.base_cnes import executar_fetch_competencia_atual

DIRETORIO_FTP = "/dissemin/publicos/CNES/200508_/Dados/LT"

if __name__ == "__main__":
    executar_fetch_competencia_atual("LT", DIRETORIO_FTP, "dbc_cnes_leitos")