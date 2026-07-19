import os
import json
import logging
import boto3
from pathlib import Path
from datetime import datetime
from botocore.exceptions import ClientError

from scripts.common.paths import BASE_DIR, PUBLISH_CACHE_DIR
from scripts.common import env

# ------------------- Kaggle -------------------
KAGGLE_DIR = env.KAGGLE_DIR
KAGGLE_JSON = env.KAGGLE_JSON

os.environ['KAGGLE_CONFIG_DIR'] = str(KAGGLE_DIR)
if KAGGLE_JSON.exists():
    os.chmod(KAGGLE_JSON, 0o600)

from kaggle.api.kaggle_api_extended import KaggleApi

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------
# MinIO 
# -----------------------------
MINIO_ENDPOINT = env.MINIO_ENDPOINT
if MINIO_ENDPOINT == "http://minio:9000":
    MINIO_ENDPOINT = "http://localhost:9000"

MINIO_ACCESS_KEY = env.MINIO_ROOT_USER
MINIO_SECRET_KEY = env.MINIO_ROOT_PASSWORD
MINIO_BUCKET = env.MINIO_BUCKET

DATASET_NAME = 'brazilian-kaggle-datahub'
DATASET_TITLE = 'Brazilian Kaggle Datahub'
FILES_TO_IGNORE = {'.gitkeep', 'raw_lake_metadados.csv'}

# Cache persistente (definido em paths.py, customizável via .env)
CACHE_DIR = PUBLISH_CACHE_DIR

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
    
    paginator = s3_client.get_paginator('list_objects_v2')
    objetos_s3 = {}  # {chave: tamanho}
    
    try:
        for page in paginator.paginate(Bucket=MINIO_BUCKET):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if Path(obj['Key']).name not in FILES_TO_IGNORE:
                        objetos_s3[obj['Key']] = obj['Size']
    except ClientError as e:
        logger.error(f"Erro ao acessar o MinIO: {e}")
        return

    if not objetos_s3:
        logger.warning(f"Nenhum arquivo encontrado no bucket {MINIO_BUCKET}. Encerrando.")
        return

    logger.info(f"Cache persistente: {CACHE_DIR}")

    # Baixa só novo/alterado, reaproveita cache quando possível
    baixados = 0
    reaproveitados = 0
    for s3_key, tamanho_remoto in objetos_s3.items():
        destino = CACHE_DIR / s3_key
        if destino.exists() and destino.stat().st_size == tamanho_remoto:
            reaproveitados += 1
            continue

        destino.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Baixando do Lake (novo/alterado): {s3_key}")
        s3_client.download_file(MINIO_BUCKET, s3_key, str(destino))
        baixados += 1

    logger.info(f"✔ {baixados} arquivo(s) baixado(s), {reaproveitados} reaproveitado(s) do cache local.")

    # Remove arquivos órfãos (não existem mais no bucket)
    chaves_esperadas = set(objetos_s3.keys())
    removidos = 0
    for caminho_local in CACHE_DIR.rglob("*"):
        if caminho_local.is_file():
            chave_relativa = str(caminho_local.relative_to(CACHE_DIR)).replace(os.sep, "/")
            if chave_relativa != "dataset-metadata.json" and chave_relativa not in chaves_esperadas:
                caminho_local.unlink()
                removidos += 1
    if removidos:
        logger.info(f"✔ {removidos} arquivo(s) órfão(s) removido(s) do cache (não existem mais no bucket).")

    # Pula se sem mudanças desde última publicação
    if baixados == 0 and removidos == 0:
        logger.info("Nenhuma novidade real desde a última publicação -- pulando o envio ao Kaggle.")
        return

    metadata_path = CACHE_DIR / "dataset-metadata.json"

    # Verifica se existe; preserva tags/descrição configuradas manualmente
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

    metadata = None
    if dataset_exists:
        logger.info("Baixando metadados existentes para preservar tags/subtítulo/descrição configurados manualmente...")
        try:
            api.dataset_metadata(dataset_id, path=str(CACHE_DIR))
            with open(metadata_path, "r") as m:
                metadata = json.load(m)

            # Trata JSON com dupla codificação (quirk da API Kaggle)
            if isinstance(metadata, str):
                logger.info("Metadata veio com codificação JSON dupla (bug conhecido da API) -- desembrulhando...")
                metadata = json.loads(metadata)

            if not isinstance(metadata, dict):
                raise TypeError(f"Metadata existente não é um dict mesmo após desembrulhar (veio como {type(metadata).__name__}).")

            metadata["id"] = dataset_id  # garante que está certo, independente do que veio
            metadata["resources"] = []  # API redetecta arquivos do cache
                                        
            logger.info("✔ Metadados existentes preservados com sucesso.")
        except Exception as e:
            logger.warning(f"Não consegui baixar metadados existentes ({e}) -- "
                            f"usando metadata mínimo. Tags/subtítulo/descrição configurados "
                            f"manualmente podem ser perdidos nesta publicação.")

    if metadata is None:
        # Primeira publicação ou fallback se download falhou
        metadata = {
            "title": DATASET_TITLE,
            "id": dataset_id,
            "licenses": [{"name": "CC0-1.0"}],
            "resources": [],
        }

    with open(metadata_path, "w") as m:
        json.dump(metadata, m, indent=4)
        m.flush()
        os.fsync(m.fileno())

    # Validação: garante metadata válido
    with open(metadata_path, "r") as m:
        conteudo_verificado = json.load(m)
    if not isinstance(conteudo_verificado, dict):
        raise TypeError(
            f"dataset-metadata.json foi escrito, mas não é um objeto JSON válido "
            f"(veio como {type(conteudo_verificado).__name__}): {conteudo_verificado!r}"
        )
    logger.info(f"✔ dataset-metadata.json validado ({len(conteudo_verificado)} campo(s)).")

    logger.info("Metadata gerado. Iniciando comunicação com o Kaggle...")

    try:
        if dataset_exists:
            api.dataset_create_version(
                folder=str(CACHE_DIR),
                version_notes=f"Automated Lake Sync - {datetime.now().strftime('%Y-%m-%d')}",
                delete_old_versions=True,
                quiet=False,
                dir_mode='zip'
            )
            logger.info(f"✔ Dataset {dataset_id} atualizado com sucesso!")
        else:
            api.dataset_create_new(
                folder=str(CACHE_DIR),
                public=True,
                quiet=False,
                dir_mode='zip'
            )
            logger.info(f"✔ Dataset {dataset_id} criado com sucesso!")

    except Exception as e:
        logger.error(f"❌ Erro na API do Kaggle: {e}")
        raise

if __name__ == "__main__":
    load_lake_to_kaggle()