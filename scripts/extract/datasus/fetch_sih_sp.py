"""SIH/SUS - SP (Serviços Profissionais, 2008-atual)."""
from scripts.extract.datasus.base_sih import executar_fetch

if __name__ == "__main__":
    executar_fetch("SP", "dbc_sih_sp")