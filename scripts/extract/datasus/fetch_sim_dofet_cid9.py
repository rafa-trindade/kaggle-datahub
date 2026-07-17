"""SIM-DOFET - Declarações de Óbitos Fetais, CID-9 (1979-1995)."""
from scripts.extract.datasus.base_sim_dofet import executar_fetch

if __name__ == "__main__":
    executar_fetch("DOFET", "/dissemin/publicos/SIM/CID9/DOFET", "dbc_sim_dofet_cid9")