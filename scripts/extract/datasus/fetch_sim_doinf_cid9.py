"""SIM - Óbitos Infantis, CID-9 (1979-1995)."""
from scripts.extract.datasus.base_sim_dofet import executar_fetch

if __name__ == "__main__":
    executar_fetch("DOINF", "/dissemin/publicos/SIM/CID9/DOFET", "dbc_sim_doinf_cid9")