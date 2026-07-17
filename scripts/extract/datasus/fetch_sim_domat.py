"""SIM-DOMAT - Declarações de Óbitos Maternos, 1996-2024. Só existe CID-10."""
from scripts.extract.datasus.base_sim_dofet import executar_fetch_multiplos

if __name__ == "__main__":
    executar_fetch_multiplos("DOMAT", [
        "/dissemin/publicos/SIM/CID10/DOFET",
        "/dissemin/publicos/SIM/PRELIM/DOFET",
    ], "dbc_sim_domat")