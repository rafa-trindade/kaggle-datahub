"""SIA/SUS - AR (APAC de Radioterapia, 2008-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("AR", "dbc_sia_ar")
