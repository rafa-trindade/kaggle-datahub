import json
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime

DATA_DIR = Path("/opt/airflow/data/raw") 
KAGGLE_JSON_PATH = Path("/home/airflow/.config/kaggle/kaggle.json")
DATASET_NAME = 'sim-br'
DATASET_TITLE = 'SIM-BR - Pipeline Diário'

ARQUIVOS_PARA_ENVIAR = {
    "raw_sistema_info_mortalidade_prelim.parquet": "raw_info_mortalidade_prelim.parquet",
    "raw_sistema_info_mortalidade.parquet": "raw_info_mortalidade.parquet",
    "raw_macroregiao_de_saude.parquet": "raw_macroregiao.parquet",
    "raw_metadados_simbr.csv": "raw_metadados_simbr.csv"
}

api = KaggleApi()
api.authenticate()

with open(KAGGLE_JSON_PATH) as f:
    kaggle_creds = json.load(f)
KAGGLE_USER = kaggle_creds["username"]
DATASET_ID = f"{KAGGLE_USER}/{DATASET_NAME}"

def preparar_pasta_dataset():
    """Copia os arquivos que queremos enviar para uma pasta temporária de upload."""
    import shutil
    temp_folder = DATA_DIR / "upload_tmp"
    if temp_folder.exists():
        shutil.rmtree(temp_folder)
    temp_folder.mkdir(parents=True, exist_ok=True)

    for local_name, kaggle_name in ARQUIVOS_PARA_ENVIAR.items():
        src = DATA_DIR / local_name
        dst = temp_folder / kaggle_name
        if src.exists():
            shutil.copy2(src, dst)
            print(f"✅ Arquivo '{src}' copiado para '{dst}'")
        else:
            print(f"⚠️ Arquivo '{src}' não encontrado, será ignorado.")

    return temp_folder

def load_raw_to_kaggle():
    """
    Cria ou atualiza o dataset público 'onco-360' no Kaggle
    apenas com os arquivos especificados em ARQUIVOS_PARA_ENVIAR.
    """
    print(f"Iniciando o carregamento para o Kaggle: {DATASET_ID}")

    temp_folder = preparar_pasta_dataset()
    metadata_path = temp_folder / "dataset-metadata.json"

    metadata = {
        "title": DATASET_TITLE,
        "id": DATASET_ID,
        "licenses": [{"name": "CC0-1.0"}],
        "resources": [],
        "version": datetime.now().strftime("%Y%m%d")
    }

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
                folder=str(temp_folder),
                version_notes=f"Update {datetime.now().strftime('%Y-%m-%d')} - New version",
                delete_old_versions=True,
                quiet=False
            )
            print(f"✅ Dataset {DATASET_ID} atualizado com sucesso!")
        else:
            api.dataset_create_new(
                folder=str(temp_folder),
                public=True,
                quiet=False
            )
            print(f"✅ Dataset {DATASET_ID} criado com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao interagir com o Kaggle: {e}")
        raise
    finally:
        import shutil
        if temp_folder.exists():
            shutil.rmtree(temp_folder)
            print(f"Pasta temporária '{temp_folder}' removida.")

if __name__ == "__main__":
    load_raw_to_kaggle()
