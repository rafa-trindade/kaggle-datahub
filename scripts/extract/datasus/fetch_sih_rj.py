"""SIH/SUS - RJ (AIH Rejeitadas, 2008-atual)."""
from scripts.extract.datasus.base_sih import executar_fetch

if __name__ == "__main__":
    executar_fetch("RJ", "dbc_sih_rj")