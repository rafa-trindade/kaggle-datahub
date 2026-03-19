from pathlib import Path
from scripts.process.datasus.base_process_dbc import processar_diretorio_dbc

CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = CURRENT_DIR.parent.parent.parent
DBC_DIR = BASE_DIR / "data" / "landing" / "dbc_datasus_sim" / "cid9"
RAW_DIR = BASE_DIR / "data" / "raw" / "datasus" / "declaracoes_de_obito"

CSV_FINAL_PATH = RAW_DIR / "raw_sim_declaracao_obito_cid09.csv"

if __name__ == "__main__":
    processar_diretorio_dbc(DBC_DIR, CSV_FINAL_PATH)