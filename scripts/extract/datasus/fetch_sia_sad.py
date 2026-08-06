"""SIA/SUS - SAD (Atenção Domiciliar, Nov/2012-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("SAD", "dbc_sia_sad")
