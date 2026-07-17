"""
Caminhos centrais do projeto, usados por todos os módulos de extract/process/load.

Antes desse módulo, cada fonte (dados_abertos, datasus...) redefinia
BASE_DIR/LANDING_DIR/RAW_DIR do zero no seu próprio base_*.py -- sempre os
mesmos 3 caminhos, reescritos em vários lugares. Centralizando aqui, mudar
a estrutura de pastas do projeto passa a ser uma alteração em um único
arquivo.

Nota: este projeto usa RAW_DIR (não PROCESSED_DIR) para a camada de dados
tratados -- convenção própria do kaggle-datahub, mantida aqui de propósito.

Nota 2: diferente do onco-360-foundation, LANDING_DIR e RAW_DIR aqui são
pastas de TRABALHO TEMPORÁRIO, não um data lake local persistido -- os
arquivos passam por elas durante o processamento, mas são apagados
depois de publicados no bucket (ver scripts.common.bucket_sync). Ficam
mkdir'adas de propósito mesmo assim, já que continuam sendo o
scratch space real durante uma execução.

Nota 3: fontes como o SIH podem acumular vários GB em LANDING_DIR durante
uma execução longa (muitos arquivos .dbc baixados antes do process rodar
e limpar). Se o disco onde o projeto mora for pequeno (típico em SSD de
sistema), dá pra apontar LANDING_DIR pra outro disco via variável de
ambiente KAGGLE_DATAHUB_LANDING_DIR, sem mudar nenhum outro código -- só
adicionar no .env: KAGGLE_DATAHUB_LANDING_DIR=D:\\kaggle-datahub-landing
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# scripts/common/paths.py -> sobe 3 níveis para chegar na raiz do projeto
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Carrega o .env AQUI, não só em env.py -- paths.py é importado antes de
# env.py em várias cadeias de import (ex: base_ftp.py importa bucket_sync,
# que importa env, que importa BASE_DIR daqui -- só que na primeira vez
# que paths.py roda, se for ANTES do load_dotenv() de env.py acontecer,
# o override de LANDING_DIR, RAW_DIR abaixo nunca vê a variável do .env, já que o
# módulo fica em cache depois da primeira execução). Chamar aqui também
# garante que funciona não importa a ordem de import.
load_dotenv(BASE_DIR / ".env")

DATA_DIR = BASE_DIR / "data"

_landing_override = os.environ.get("KAGGLE_DATAHUB_LANDING_DIR")
_raw_override = os.environ.get("KAGGLE_DATAHUB_RAW_DIR")

LANDING_DIR = Path(_landing_override) if _landing_override else DATA_DIR / "landing"
RAW_DIR = Path(_raw_override) if _raw_override else DATA_DIR / "raw"

LANDING_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)