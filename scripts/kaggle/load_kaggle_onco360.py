import json
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime
import tempfile # Novo
import shutil # Novo

DATA_DIR = Path("/opt/airflow/data/raw") 
KAGGLE_JSON_PATH = Path("/home/airflow/.config/kaggle/kaggle.json")
DATASET_NAME = 'onco-360'
DATASET_TITLE = 'Onco-360 - Pipeline Diário'

FILES_TO_IGNORE = {
    'dataset-metadata.json', 
    'raw_sim_metadados.csv', 
    'raw_sim_causas_externas_prelim.parquet', 
    'raw_sim_causas_externas.parquet'
}

api = KaggleApi()
api.authenticate()

with open(KAGGLE_JSON_PATH) as f:
    kaggle_creds = json.load(f)
KAGGLE_USER = kaggle_creds["username"]
DATASET_ID = f"{KAGGLE_USER}/{DATASET_NAME}"

def load_raw_to_kaggle():
    """
    Cria ou atualiza o dataset público 'onco-360' no Kaggle com
    os arquivos da pasta data, ignorando a lista FILES_TO_IGNORE.
    """
    print(f"Iniciando o carregamento para o Kaggle: {DATASET_ID}")

    files_to_upload = [
        f for f in DATA_DIR.iterdir() 
        if f.is_file() and f.name not in FILES_TO_IGNORE
    ]

    if not files_to_upload:
        print(f"Aviso: Nenhum arquivo encontrado para upload em {DATA_DIR}. Encerrando.")
        return

    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        print(f"Diretório temporário criado para upload: {temp_dir}")

        for f in files_to_upload:
            shutil.copy(f, temp_dir / f.name)
            print(f"Copiando arquivo para temporário: {f.name}")

        metadata = {
            "title": DATASET_TITLE,
            "id": DATASET_ID,
            "licenses": [{"name": "CC0-1.0"}],
            "resources": [],
            "version": datetime.now().strftime("%Y%m%d")
        }
        metadata_path = temp_dir / "dataset-metadata.json"
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
                    folder=str(temp_dir), 
                    version_notes=f"Update {datetime.now().strftime('%Y-%m-%d')} - New version",
                    delete_old_versions=True,
                    quiet=False
                )
                print(f"✅ Dataset {DATASET_ID} atualizado com sucesso!")
            else:
                api.dataset_create_new(
                    folder=str(temp_dir), 
                    public=True,
                    quiet=False
                )
                print(f"✅ Dataset {DATASET_ID} criado com sucesso!")

        except Exception as e:
            print(f"❌ Erro ao interagir com o Kaggle: {e}")
            raise

        print(f"Diretório temporário {temp_dir} removido.")


if __name__ == "__main__":
    load_raw_to_kaggle()