"""SIA/SUS - AM (APAC de Medicamentos, 2008-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("AM", "dbc_sia_am")
