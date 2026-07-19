"""
Caminhos centrais do projeto.

LANDING_DIR e RAW_DIR são pastas de trabalho temporário -- arquivos são
apagados após publicação no bucket. Customize via .env se necessário:
  KAGGLE_DATAHUB_LANDING_DIR (temporário, pode usar disco diferente)
  KAGGLE_DATAHUB_PUBLISH_CACHE_DIR (persistente, cache de publicações)
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# scripts/common/paths.py -> sobe 3 níveis para chegar na raiz do projeto
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Carrega .env aqui para garantir que os overrides funcionem em qualquer ordem de import
load_dotenv(BASE_DIR / ".env")

DATA_DIR = BASE_DIR / "data"

_landing_override = os.environ.get("KAGGLE_DATAHUB_LANDING_DIR")
LANDING_DIR = Path(_landing_override) if _landing_override else DATA_DIR / "landing"

_publish_cache_override = os.environ.get("KAGGLE_DATAHUB_PUBLISH_CACHE_DIR")
PUBLISH_CACHE_DIR = Path(_publish_cache_override) if _publish_cache_override else DATA_DIR / "kaggle_publish_cache"

RAW_DIR = DATA_DIR / "raw"

LANDING_DIR.mkdir(parents=True, exist_ok=True)
PUBLISH_CACHE_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)