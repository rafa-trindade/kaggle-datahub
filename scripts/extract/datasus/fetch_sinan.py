"""SINAN - todos os agravos de notificação compulsória configurados em
scripts.config.agravos_sinan. Ver base_sinan.py."""
from scripts.extract.datasus.base_sinan import executar_fetch_todos_agravos
from scripts.config.agravos_sinan import AGRAVOS_SINAN

if __name__ == "__main__":
    executar_fetch_todos_agravos(AGRAVOS_SINAN)