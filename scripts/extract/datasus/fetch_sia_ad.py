"""SIA/SUS - AD (APAC de Laudos Diversos, 2008-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("AD", "dbc_sia_ad")
