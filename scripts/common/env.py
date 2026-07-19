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