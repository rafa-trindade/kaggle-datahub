"""SIM - Óbitos Infantis, CID-10 (1996-atual)."""
from scripts.extract.datasus.base_sim_dofet import executar_fetch_multiplos

if __name__ == "__main__":
    executar_fetch_multiplos("DOINF", [
        "/dissemin/publicos/SIM/CID10/DOFET",
        "/dissemin/publicos/SIM/PRELIM/DOFET",
    ], "dbc_sim_doinf_cid10")