"""SIH/SUS - RD (AIH Reduzida, 2008-atual)."""
from scripts.extract.datasus.base_sih import executar_fetch

if __name__ == "__main__":
    executar_fetch("RD", "dbc_sih_rd")