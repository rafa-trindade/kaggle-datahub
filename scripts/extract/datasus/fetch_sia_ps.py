"""SIA/SUS - PS (Psicossocial, Jan/2013-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("PS", "dbc_sia_ps")
