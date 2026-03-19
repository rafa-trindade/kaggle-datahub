from pathlib import Path
from scripts.process.datasus.base_process_dbc import processar_diretorio_dbc

CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = CURRENT_DIR.parent.parent.parent
DBC_DIR = BASE_DIR / "data" / "landing" / "dbc_sim_causas_externas" / "cid9"
RAW_DIR = BASE_DIR / "data" / "raw" / "datasus" / "declaracoes_de_obito_causas_externas"

CSV_FINAL_PATH = RAW_DIR / "raw_sim_causas_externas_cid09.csv"

if __name__ == "__main__":
    processar_diretorio_dbc(DBC_DIR, CSV_FINAL_PATH)