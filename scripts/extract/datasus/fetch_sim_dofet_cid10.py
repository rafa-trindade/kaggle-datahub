"""SIM-DOFET - Declarações de Óbitos Fetais, CID-10 (1996-2024)."""
from scripts.extract.datasus.base_sim_dofet import executar_fetch_multiplos

if __name__ == "__main__":
    executar_fetch_multiplos("DOFET", [
        "/dissemin/publicos/SIM/CID10/DOFET",
        "/dissemin/publicos/SIM/PRELIM/DOFET",
    ], "dbc_sim_dofet_cid10")