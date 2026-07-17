"""SIH/SUS - RJ (AIH Rejeitadas) -- internações não aprovadas para
pagamento, 2008-atual. Ver base_sih.py."""
from scripts.extract.datasus.base_sih import executar_fetch

if __name__ == "__main__":
    executar_fetch("RJ", "dbc_sih_rj")