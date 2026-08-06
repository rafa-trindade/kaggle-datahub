"""SIA/SUS - AQ (APAC de Quimioterapia, 2008-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("AQ", "dbc_sia_aq")
