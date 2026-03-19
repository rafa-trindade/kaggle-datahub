import os
import json
import logging
import tempfile
import boto3
from pathlib import Path
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ------------------- Caminhos e Diretórios -------------------
CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = CURRENT_DIR.parent.parent

# --- O PULO DO GATO PARA AS CREDENCIAIS DO KAGGLE ---
KAGGLE_DIR = BASE_DIR / ".kaggle"
KAGGLE_JSON = KAGGLE_DIR / "kaggle.json"
os.environ['KAGGLE_CONFIG_DIR'] = str(KAGGLE_DIR)

# Corrige o aviso de permissão do kaggle.json automaticamente (chmod 600)
if KAGGLE_JSON.exists():
    os.chmod(KAGGLE_JSON, 0o600)

from kaggle.api.kaggle_api_extended import KaggleApi

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------
# Configurações MinIO e Kaggle
# -----------------------------
load_dotenv(BASE_DIR / ".env")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
if MINIO_ENDPOINT == "http://minio:9000":
    MINIO_ENDPOINT = "http://localhost:9000"

MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET")

DATASET_NAME = 'brazilian-kaggle-datahub'
DATASET_TITLE = 'Brazilian Kaggle Datahub'
FILES_TO_IGNORE = {'.gitkeep', 'raw_lake_metadados.csv'}

# ----------------------------
# S3 / MinIO Client
# ----------------------------
def criar_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

# ----------------------------
# Main Process
# ----------------------------
def load_lake_to_kaggle():
    logger.info("Autenticando na API do Kaggle...")
    api = KaggleApi()
    api.authenticate()
    
    kaggle_user = api.get_config_value('username')
    dataset_id = f"{kaggle_user}/{DATASET_NAME}"
    
    logger.info(f"Conectando ao Data Lake (MinIO) no bucket: {MINIO_BUCKET}")
    s3_client = criar_s3_client()
    
    # Lista todos os arquivos no MinIO (usando paginator caso o lake fique muito grande no futuro)
    paginator = s3_client.get_paginator('list_objects_v2')
    objetos_s3 = []
    
    try:
        for page in paginator.paginate(Bucket=MINIO_BUCKET):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if Path(obj['Key']).name not in FILES_TO_IGNORE:
                        objetos_s3.append(obj['Key'])
    except ClientError as e:
        logger.error(f"Erro ao acessar o MinIO: {e}")
        return

    if not objetos_s3:
        logger.warning(f"Nenhum arquivo encontrado no bucket {MINIO_BUCKET}. Encerrando.")
        return

    # Inicia a criação do pacote para o Kaggle
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        logger.info(f"Diretório temporário criado: {temp_dir}")

        # Puxa os arquivos do MinIO para o diretório temporário, mantendo as pastas
        for s3_key in objetos_s3:
            destino = temp_dir / s3_key
            destino.parent.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Baixando do Lake: {s3_key}")
            s3_client.download_file(MINIO_BUCKET, s3_key, str(destino))

        # Cria o metadado exigido pelo Kaggle
        metadata = {
            "title": DATASET_TITLE,
            "id": dataset_id,
            "licenses": [{"name": "CC0-1.0"}],
            "resources": [],
            "version": datetime.now().strftime("%Y%m%d")
        }
        
        metadata_path = temp_dir / "dataset-metadata.json"
        with open(metadata_path, "w") as m:
            json.dump(metadata, m, indent=4)
            
        logger.info("Metadata gerado. Iniciando comunicação com o Kaggle...")
        
        # Publicação no Kaggle (com a correção do erro 403/404)
        try:
            try:
                api.dataset_list_files(dataset_id)
                dataset_exists = True
                logger.info(f"Dataset {dataset_id} encontrado. Iniciando atualização da versão...")
            except Exception as e:
                erro_str = str(e)
                if "404" in erro_str or "403" in erro_str:
                    dataset_exists = False
                    logger.info(f"Dataset {dataset_id} não existe ou é privado. Criando novo dataset...")
                else:
                    raise

            if dataset_exists:
                api.dataset_create_version(
                    folder=str(temp_dir), 
                    version_notes=f"Automated Lake Sync - {datetime.now().strftime('%Y-%m-%d')}",
                    delete_old_versions=True,
                    quiet=False,
                    dir_mode='zip'  # <--- MÁGICA AQUI
                )
                logger.info(f"✔ Dataset {dataset_id} atualizado com sucesso!")
            else:
                api.dataset_create_new(
                    folder=str(temp_dir), 
                    public=True,
                    quiet=False,
                    dir_mode='zip'  # <--- MÁGICA AQUI
                )
                logger.info(f"✔ Dataset {dataset_id} criado com sucesso!")

        except Exception as e:
            logger.error(f"❌ Erro na API do Kaggle: {e}")
            raise

if __name__ == "__main__":
    load_lake_to_kaggle()