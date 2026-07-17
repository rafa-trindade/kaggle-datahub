"""SIH/SUS - SP (Serviços Profissionais) -- atos médicos realizados
durante internações, 2008-atual. Ver base_sih.py."""
from scripts.extract.datasus.base_sih import executar_fetch

if __name__ == "__main__":
    executar_fetch("SP", "dbc_sih_sp")