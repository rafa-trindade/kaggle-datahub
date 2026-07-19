"""
SIM - Mortalidade de Residentes no Exterior (2013-atual).

Só existe CID-10 (a série começa em 2013).
"""
from scripts.extract.datasus.base_sim_dofet import executar_fetch_multiplos

if __name__ == "__main__":
    executar_fetch_multiplos("DOREXT", [
        "/dissemin/publicos/SIM/CID10/DOFET",
        "/dissemin/publicos/SIM/PRELIM/DOFET",
    ], "dbc_sim_dorext")