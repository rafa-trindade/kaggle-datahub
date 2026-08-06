"""SIA/SUS - AN (APAC de Nefrologia, 2008-2014)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("AN", "dbc_sia_an")
