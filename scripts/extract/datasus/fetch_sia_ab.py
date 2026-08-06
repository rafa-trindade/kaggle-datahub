"""SIA/SUS - AB (APAC de Cirurgia Bariátrica, 2008-Mar/2013)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("AB", "dbc_sia_ab")
