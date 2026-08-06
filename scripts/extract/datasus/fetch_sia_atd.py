"""SIA/SUS - ATD (APAC de Tratamento Dialítico, Jun/2014-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("ATD", "dbc_sia_atd")
