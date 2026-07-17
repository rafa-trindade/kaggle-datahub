"""CNES - Habilitações -- todas as especialidades, sem filtro (diferente
do onco-360-foundation, que filtra só oncologia). Competência mais
recente disponível (retrato, não série histórica)."""
from scripts.extract.datasus.base_cnes import executar_fetch_competencia_atual

DIRETORIO_FTP = "/dissemin/publicos/CNES/200508_/Dados/HB"

if __name__ == "__main__":
    executar_fetch_competencia_atual("HB", DIRETORIO_FTP, "dbc_cnes_habilitacoes")