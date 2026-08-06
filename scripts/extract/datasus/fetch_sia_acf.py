"""SIA/SUS - ACF (APAC de Confecção de Fístula Arteriovenosa, Jun/2014-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("ACF", "dbc_sia_acf")
