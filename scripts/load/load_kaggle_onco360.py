import json
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime

DATA_DIR = Path("/opt/airflow/data/raw") 
KAGGLE_JSON_PATH = Path("/home/airflow/.config/kaggle/kaggle.json")
DATASET_NAME = 'onco-360'
DATASET_TITLE = 'Onco-360 - Pipeline Diário'

api = KaggleApi()
api.authenticate()

with open(KAGGLE_JSON_PATH) as f:
    kaggle_creds = json.load(f)
KAGGLE_USER = kaggle_creds["username"]
DATASET_ID = f"{KAGGLE_USER}/{DATASET_NAME}"

def load_raw_to_kaggle():
    """
    Cria ou atualiza o dataset público 'onco-360' no Kaggle com
    os arquivos da pasta data/onco-360, ignorando raw_metadados_simbr.csv
    """
    print(f"Iniciando o carregamento para o Kaggle: {DATASET_ID}")

    metadata = {
        "title": DATASET_TITLE,
        "id": DATASET_ID,
        "licenses": [{"name": "CC0-1.0"}],
        "resources": [],
        "version": datetime.now().strftime("%Y%m%d")
    }

    files_to_upload = [
        f for f in DATA_DIR.iterdir() 
        if f.is_file() and f.name not in ['dataset-metadata.json', 'raw_metadados_simbr.csv']
    ]

    if not files_to_upload:
        print(f"Aviso: Nenhum arquivo encontrado em {DATA_DIR}. Encerrando o processo de upload.")
        return

    metadata_path = DATA_DIR / "dataset-metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)
    print(f"Metadata criado em: {metadata_path}")

    try:
        try:
            api.dataset_list_files(DATASET_ID)
            dataset_exists = True
            print(f"Dataset {DATASET_ID} já existe. Tentando atualizar...")
        except Exception as e:
            if "404 - Not Found" in str(e):
                dataset_exists = False
                print(f"Dataset {DATASET_ID} não existe. Tentando criar...")
            else:
                raise

        if dataset_exists:
            api.dataset_create_version(
                folder=str(DATA_DIR), 
                version_notes=f"Update {datetime.now().strftime('%Y-%m-%d')} - New version",
                delete_old_versions=True,
                quiet=False
            )
            print(f"✅ Dataset {DATASET_ID} atualizado com sucesso!")
        else:
            api.dataset_create_new(
                folder=str(DATA_DIR), 
                public=True,
                quiet=False
            )
            print(f"✅ Dataset {DATASET_ID} criado com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao interagir com o Kaggle: {e}")
        raise
    finally:
        if metadata_path.exists():
            metadata_path.unlink()
            print(f"Arquivo temporário de metadata {metadata_path} removido.")


if __name__ == "__main__":
    load_raw_to_kaggle()
