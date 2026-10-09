"""
Variáveis de ambiente centrais do projeto -- lidas uma única vez aqui
"""
import os
from pathlib import Path
from dotenv import load_dotenv

from scripts.common.paths import BASE_DIR

load_dotenv(BASE_DIR / ".env")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET")

KAGGLE_DIR = BASE_DIR / ".kaggle"
KAGGLE_JSON = KAGGLE_DIR / "kaggle.json"
# Onde fica o "lake": "minio" (bucket S3/MinIO, padrão) ou "local" (a própria
# pasta de publicação do Kaggle, KAGGLE_DATAHUB_PUBLISH_CACHE_DIR).
DATAHUB_STORAGE = os.environ.get("DATAHUB_STORAGE", "minio").strip().lower()
if DATAHUB_STORAGE not in ("minio", "local"):
    raise ValueError(f"DATAHUB_STORAGE inválido: '{DATAHUB_STORAGE}' (use 'minio' ou 'local').")
MODO_LOCAL = DATAHUB_STORAGE == "local"
