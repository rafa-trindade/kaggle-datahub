"""SIA/SUS - ABO (APAC de Acompanhamento Pós Cirurgia Bariátrica, Abr/2013-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch

if __name__ == "__main__":
    executar_fetch("ABO", "dbc_sia_abo")
