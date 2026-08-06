"""CIHA - Comunicação de Informação Hospitalar e Ambulatorial (2011-atual)."""
from scripts.extract.datasus.base_ciha import executar_fetch

if __name__ == "__main__":
    executar_fetch("CIHA", "dbc_ciha")
