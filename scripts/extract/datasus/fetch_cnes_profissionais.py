"""CNES - Profissionais -- todos os profissionais de saúde cadastrados
por estabelecimento (CBO, carga horária, forma de contratação).
Competência mais recente (retrato, não série histórica)."""
from scripts.extract.datasus.base_cnes import executar_fetch_competencia_atual

DIRETORIO_FTP = "/dissemin/publicos/CNES/200508_/Dados/PF"

if __name__ == "__main__":
    executar_fetch_competencia_atual("PF", DIRETORIO_FTP, "dbc_cnes_profissionais")