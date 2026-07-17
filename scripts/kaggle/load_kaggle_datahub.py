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
# Configurações MinIO -- lidas de env.py, não daqui (era o padrão
# antigo, cada script lendo as mesmas 4 variáveis por conta própria)
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

# Pasta PERSISTENTE de cache (não temporária) -- definida em
# scripts/common/paths.py (PUBLISH_CACHE_DIR), configurável via
# KAGGLE_DATAHUB_PUBLISH_CACHE_DIR no .env. Mantida entre execuções,
# pra só baixar de novo o que realmente mudou (comparando tamanho
# local vs remoto, que o list_objects_v2 já devolve de graça, sem
# chamada extra por arquivo).
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

    # Baixa só o que é novo ou mudou de tamanho desde a última
    # publicação -- reaproveita o que já está no cache local
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

    # Limpa do cache local qualquer arquivo que não existe mais no
    # bucket (evita publicar dado obsoleto/removido na origem)
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

    # Se nada foi baixado nem removido, o bucket está idêntico ao que
    # já está no cache local -- ou seja, idêntico ao que já foi
    # publicado da última vez que essa execução rodou até o fim com
    # sucesso. Sem isso, cada execução reenviava os ~13GB inteiros pro
    # Kaggle mesmo sem nenhuma novidade real (confirmado empiricamente:
    # duas execuções seguidas, mesmo conteúdo, dois reuploads completos).
    if baixados == 0 and removidos == 0:
        logger.info("Nenhuma novidade real desde a última publicação -- pulando o envio ao Kaggle.")
        return

    metadata_path = CACHE_DIR / "dataset-metadata.json"

    # Checa se o dataset já existe ANTES de decidir o metadata --
    # se existir, tenta preservar tags/subtítulo/descrição já
    # configurados manualmente no Kaggle, em vez de sobrescrever
    # com um metadata mínimo do zero (bug identificado: isso
    # apagava configurações manuais a cada publicação).
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

            # Bug conhecido da API do Kaggle: às vezes devolve o
            # metadata com codificação JSON DUPLA -- o arquivo contém
            # uma STRING que, quando decodificada de novo, é o objeto
            # real. Sem tratar isso, perderíamos as tags/descrição já
            # configuradas manualmente por engano (não é erro de
            # verdade, é só uma camada extra de codificação).
            if isinstance(metadata, str):
                logger.info("Metadata veio com codificação JSON dupla (bug conhecido da API) -- desembrulhando...")
                metadata = json.loads(metadata)

            if not isinstance(metadata, dict):
                raise TypeError(f"Metadata existente não é um dict mesmo após desembrulhar (veio como {type(metadata).__name__}).")

            metadata["id"] = dataset_id  # garante que está certo, independente do que veio
            metadata["resources"] = []  # deixa a API redetectar a partir dos arquivos
                                          # realmente presentes no cache, evita referenciar
                                          # arquivo antigo que não existe mais
            logger.info("✔ Metadados existentes preservados com sucesso.")
        except Exception as e:
            logger.warning(f"Não consegui baixar metadados existentes ({e}) -- "
                            f"usando metadata mínimo. Tags/subtítulo/descrição configurados "
                            f"manualmente podem ser perdidos nesta publicação.")

    if metadata is None:
        # Primeira publicação (dataset novo) ou fallback se o
        # download de metadata existente falhou.
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

    # Validação defensiva: confirma que o arquivo que acabamos de
    # escrever realmente contém um objeto JSON (dict), não uma string
    # ou outra coisa -- se isso falhar, o erro vai ser bem mais claro
    # do que o "string indices must be integers" que a biblioteca do
    # Kaggle dá quando lê um metadata malformado.
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