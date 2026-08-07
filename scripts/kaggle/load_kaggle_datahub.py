import os
import json
import logging
import boto3
from pathlib import Path
from datetime import datetime
from botocore.exceptions import ClientError

from scripts.common.paths import BASE_DIR, PUBLISH_CACHE_DIR
from scripts.common import env

# Kaggle usa tempfile.mkdtemp() internamente (respeita TEMP/TMP, não .env)
# Necessário pra evitar encher disco C: com zips grandes
_temp_dir_kaggle = PUBLISH_CACHE_DIR.parent / "_temp_zip"
_temp_dir_kaggle.mkdir(parents=True, exist_ok=True)
os.environ['TEMP'] = str(_temp_dir_kaggle)
os.environ['TMP'] = str(_temp_dir_kaggle)

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
# Configurações MinIO
# -----------------------------
MINIO_ENDPOINT = env.MINIO_ENDPOINT
if MINIO_ENDPOINT == "http://minio:9000":
    MINIO_ENDPOINT = "http://localhost:9000"

MINIO_ACCESS_KEY = env.MINIO_ROOT_USER
MINIO_SECRET_KEY = env.MINIO_ROOT_PASSWORD
MINIO_BUCKET = env.MINIO_BUCKET

FILES_TO_IGNORE = {'.gitkeep', 'raw_lake_metadados.csv'}

# ---------------------------------------------------------------------------
# Roteamento de datasets
# ---------------------------------------------------------------------------
# O PA tem cache e dataset separados (prefixo 'producao_ambulatorial/') devido ao limite de 200GB.
# Os demais dados vão para o dataset Principal. As descrições manuais do Kaggle são preservadas.
DATASET_PRINCIPAL_SLUG = 'brazilian-kaggle-datahub'
DATASET_PRINCIPAL_TITULO = 'Brazilian Kaggle Datahub'

DATASET_PA_SLUG = 'sia-producao-ambulatorial'
DATASET_PA_TITULO = 'SIA - Produção Ambulatorial (DATASUS)'

# Prefixo do bucket que pertence ao dataset do PA.
PREFIXO_PA = 'producao_ambulatorial/'


# ----------------------------
# S3 / MinIO Client
# ----------------------------
def criar_s3_client():
    # Cliente S3 simples sem Config customizada para evitar erros de streaming em arquivos gigantes.
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _baixar_com_retry(s3_client, s3_key: str, destino: Path, tamanho_esperado: int,
                      tentativas: int = 3):
    """Baixa um objeto via download_file com retry simples para quedas transitórias."""
    import time
    for tentativa in range(1, tentativas + 1):
        try:
            s3_client.download_file(MINIO_BUCKET, s3_key, str(destino))
            return True
        except Exception as e:
            logger.warning(
                f"   ↳ falha ao baixar {s3_key} (tentativa {tentativa}/{tentativas}): "
                f"{type(e).__name__}: {e}"
            )
            try:
                if destino.exists():
                    destino.unlink()
            except OSError:
                pass
            if tentativa < tentativas:
                time.sleep(3 * tentativa)
    return False


def _listar_objetos_bucket(s3_client) -> dict:
    """Lista {chave: tamanho} de todo o bucket, ignorando arquivos de controle."""
    paginator = s3_client.get_paginator('list_objects_v2')
    objetos = {}
    for page in paginator.paginate(Bucket=MINIO_BUCKET):
        for obj in page.get('Contents', []):
            if Path(obj['Key']).name not in FILES_TO_IGNORE:
                objetos[obj['Key']] = obj['Size']
    return objetos


def _descricao_inicial(qual: str, kaggle_user: str) -> str:
    """Gera descrição mínima com link cruzado para a primeira publicação (preservada posteriormente)."""
    url_principal = f"https://www.kaggle.com/datasets/{kaggle_user}/{DATASET_PRINCIPAL_SLUG}"
    url_pa = f"https://www.kaggle.com/datasets/{kaggle_user}/{DATASET_PA_SLUG}"
    if qual == "pa":
        return (
            "Produção Ambulatorial do SUS (SIA/SUS - DATASUS), série desde Jul/1994, "
            "particionada por competência mensal (um parquet por ano-mês).\n\n"
            f"Faz parte do projeto Brazilian Kaggle Datahub: {url_principal}"
        )
    return (
        "Dados públicos de saúde do Brasil (DATASUS, IBGE e outros), consolidados "
        "em Parquet.\n\n"
        f"A Produção Ambulatorial (SIA-PA), pelo seu volume, fica num dataset dedicado: {url_pa}"
    )


def _caminho_local_no_cache(s3_key: str, reorganizar_pa: bool) -> str:
    """Decide o caminho relativo no cache. Se reorganizar_pa=True, agrupa os parquets por ano."""
    if not reorganizar_pa:
        return s3_key

    import re
    nome = s3_key.rsplit("/", 1)[-1]
    m = re.search(r"_(\d{4})\d{2}\.parquet$", nome)
    if m:
        ano = m.group(1)
        return f"{ano}/{nome}"
    # o manifesto vai para uma subpasta própria, deixando a raiz do dataset
    # limpa (só o metadados CSV abre direto na página do Kaggle)
    if nome == "_manifest.json":
        return f"manifesto/{nome}"
    # demais arquivos de raiz (ex.: o metadados CSV) ficam na raiz do dataset
    return nome


def _sincronizar_cache(s3_client, objetos_do_dataset: dict, cache_dir: Path,
                       reorganizar_pa: bool = False):
    """Sincroniza arquivos locais com o Lake (baixa novidades, remove órfãos)."""
    cache_dir.mkdir(parents=True, exist_ok=True)

    # mapa: caminho_relativo_no_cache -> (s3_key, tamanho)
    mapa_local = {}
    for s3_key, tam in objetos_do_dataset.items():
        rel = _caminho_local_no_cache(s3_key, reorganizar_pa)
        mapa_local[rel] = (s3_key, tam)

    baixados = 0
    reaproveitados = 0
    for rel, (s3_key, tamanho_remoto) in mapa_local.items():
        destino = cache_dir / rel
        if destino.exists() and destino.stat().st_size == tamanho_remoto:
            reaproveitados += 1
            continue
        destino.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Baixando do Lake (novo/alterado): {s3_key}")
        if not _baixar_com_retry(s3_client, s3_key, destino, tamanho_remoto):
            raise RuntimeError(
                f"Não foi possível baixar {s3_key} após várias tentativas. "
                f"Verifique a conexão com o MinIO e rode novamente -- os arquivos já "
                f"baixados ficam no cache e serão reaproveitados."
            )
        baixados += 1

    logger.info(f"✔ {baixados} baixado(s), {reaproveitados} reaproveitado(s) do cache local.")

    # Remove do cache o que não pertence mais a este dataset (órfãos), preservando
    # os arquivos de controle nossos.
    ARQUIVOS_DE_CONTROLE = {"dataset-metadata.json", ".ultima_publicacao_sucesso"}
    chaves_esperadas = set(mapa_local.keys())
    removidos = 0
    for caminho_local in cache_dir.rglob("*"):
        if caminho_local.is_file():
            chave_relativa = str(caminho_local.relative_to(cache_dir)).replace(os.sep, "/")
            if chave_relativa not in ARQUIVOS_DE_CONTROLE and chave_relativa not in chaves_esperadas:
                caminho_local.unlink()
                removidos += 1
    if removidos:
        logger.info(f"✔ {removidos} órfão(s) removido(s) do cache (não pertencem mais a este dataset).")
        # remove pastas de ano que ficaram vazias após a limpeza
        for d in sorted(cache_dir.rglob("*"), reverse=True):
            if d.is_dir() and not any(d.iterdir()):
                try:
                    d.rmdir()
                except OSError:
                    pass

    return baixados, removidos


def _preparar_metadata(api, dataset_id: str, titulo: str, cache_dir: Path,
                       qual: str, kaggle_user: str):
    """Prepara dataset-metadata.json preservando descrição/tags manuais e retorna se o dataset existe (bool)."""
    metadata_path = cache_dir / "dataset-metadata.json"

    try:
        api.dataset_list_files(dataset_id)
        dataset_exists = True
        logger.info(f"Dataset {dataset_id} encontrado. Atualizando versão...")
    except Exception as e:
        erro_str = str(e)
        if "404" in erro_str or "403" in erro_str:
            dataset_exists = False
            logger.info(f"Dataset {dataset_id} não existe ou é privado. Criando novo...")
        else:
            raise

    metadata = None
    if dataset_exists:
        logger.info("Baixando metadados existentes para preservar descrição/tags manuais...")
        try:
            api.dataset_metadata(dataset_id, path=str(cache_dir))
            with open(metadata_path, "r") as m:
                metadata = json.load(m)
            if isinstance(metadata, str):
                logger.info("Metadata com codificação JSON dupla (quirk da API) -- desembrulhando...")
                metadata = json.loads(metadata)
            if not isinstance(metadata, dict):
                raise TypeError(f"Metadata não é dict mesmo após desembrulhar ({type(metadata).__name__}).")
            metadata["id"] = dataset_id
            metadata["resources"] = []
            logger.info("✔ Metadados existentes preservados.")
        except Exception as e:
            logger.warning(f"Não consegui preservar metadados existentes ({e}) -- usando mínimo.")

    if metadata is None:
        metadata = {
            "title": titulo,
            "id": dataset_id,
            "licenses": [{"name": "CC0-1.0"}],
            "description": _descricao_inicial(qual, kaggle_user),
            "resources": [],
        }

    with open(metadata_path, "w") as m:
        json.dump(metadata, m, indent=4)
        m.flush()
        os.fsync(m.fileno())

    with open(metadata_path, "r") as m:
        verificado = json.load(m)
    if not isinstance(verificado, dict):
        raise TypeError(f"dataset-metadata.json inválido: {verificado!r}")
    logger.info(f"✔ dataset-metadata.json validado ({len(verificado)} campo(s)).")

    return dataset_exists


def _publicar_dataset(api, s3_client, *, qual: str, dataset_slug: str, titulo: str,
                      objetos_do_dataset: dict, cache_dir: Path, kaggle_user: str,
                      forcar: bool = False):
    """Sincroniza cache + publica UM dataset (só os seus arquivos).

    forcar=True republica mesmo sem mudanças detectadas (útil se o cache já
    está sincronizado mas a última publicação ao Kaggle falhou).
    """
    dataset_id = f"{kaggle_user}/{dataset_slug}"
    logger.info(f"===== Publicando dataset: {dataset_id} =====")

    if not objetos_do_dataset:
        logger.warning(f"Nenhum arquivo para o dataset {dataset_id}. Pulando.")
        return

    logger.info(f"Cache: {cache_dir} ({len(objetos_do_dataset)} arquivo(s) esperado(s))")

    baixados, removidos = _sincronizar_cache(
        s3_client, objetos_do_dataset, cache_dir,
        reorganizar_pa=(qual == "pa"),
    )

    marcador_sucesso = cache_dir / ".ultima_publicacao_sucesso"
    if (baixados > 0 or removidos > 0) and marcador_sucesso.exists():
        marcador_sucesso.unlink()
    ultima_ok = marcador_sucesso.exists()

    if not forcar and baixados == 0 and removidos == 0 and ultima_ok:
        logger.info("Nenhuma novidade desde a última publicação -- pulando envio ao Kaggle.")
        return
    if forcar and baixados == 0 and removidos == 0 and ultima_ok:
        logger.info("Sem novidade, mas --forcar ativo -- republicando por solicitação.")
    if baixados == 0 and removidos == 0 and not ultima_ok:
        logger.info("Cache atualizado, mas última publicação não confirmada -- publicando por garantia.")

    dataset_exists = _preparar_metadata(api, dataset_id, titulo, cache_dir, qual, kaggle_user)

    logger.info("Iniciando comunicação com o Kaggle...")
    try:
        if dataset_exists:
            api.dataset_create_version(
                folder=str(cache_dir),
                version_notes=f"Automated Lake Sync - {datetime.now().strftime('%Y-%m-%d')}",
                delete_old_versions=True,
                quiet=False,
                dir_mode='zip',
            )
            logger.info(f"✔ Dataset {dataset_id} atualizado!")
        else:
            api.dataset_create_new(
                folder=str(cache_dir),
                public=True,
                quiet=False,
                dir_mode='zip',
            )
            logger.info(f"✔ Dataset {dataset_id} criado!")
        marcador_sucesso.write_text(datetime.now().isoformat())
    except Exception as e:
        logger.error(f"❌ Erro na API do Kaggle para {dataset_id}: {e}")
        raise


def _migrar_cache_antigo_para_principal(cache_principal: Path):
    """Migração de legado: move o cache antigo da raiz para o subdiretório 'principal/'."""
    raiz = PUBLISH_CACHE_DIR
    marca_antiga = (raiz / "dataset-metadata.json").exists() or \
                   (raiz / ".ultima_publicacao_sucesso").exists()
    if not marca_antiga:
        return

    logger.info("Detectado cache no layout antigo (achatado na raiz) -- migrando para principal/ ...")
    cache_principal.mkdir(parents=True, exist_ok=True)
    reservados = {"principal", "pa"}  # não mover os novos subdirs
    for item in list(raiz.iterdir()):
        if item.name in reservados:
            continue
        destino = cache_principal / item.name
        try:
            if destino.exists():
                # já existe no destino: remove o antigo da raiz para não virar órfão
                if item.is_dir():
                    import shutil
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    item.unlink()
            else:
                item.rename(destino)
        except OSError as e:
            logger.warning(f"  não consegui migrar {item.name}: {e}")
    logger.info("✔ Migração do cache antigo concluída.")


# ----------------------------
# Main Process
# ----------------------------
def load_lake_to_kaggle(alvo: str = "ambos", forcar: bool = False):
    """Publica o Data Lake nos datasets Kaggle.

    alvo: 'principal', 'pa' ou 'ambos' (padrão) -- qual dataset publicar.
    forcar: republica mesmo sem mudanças detectadas.
    """
    logger.info("Autenticando na API do Kaggle...")
    api = KaggleApi()
    api.authenticate()
    kaggle_user = api.get_config_value('username')

    logger.info(f"Conectando ao Data Lake (MinIO) no bucket: {MINIO_BUCKET}")
    s3_client = criar_s3_client()

    try:
        objetos_s3 = _listar_objetos_bucket(s3_client)
    except ClientError as e:
        logger.error(f"Erro ao acessar o MinIO: {e}")
        return

    if not objetos_s3:
        logger.warning(f"Nenhum arquivo encontrado no bucket {MINIO_BUCKET}. Encerrando.")
        return

    # Particiona os objetos do bucket entre os dois datasets por prefixo.
    objetos_pa = {k: v for k, v in objetos_s3.items() if k.startswith(PREFIXO_PA)}
    objetos_principal = {k: v for k, v in objetos_s3.items() if not k.startswith(PREFIXO_PA)}

    logger.info(f"Roteamento: {len(objetos_principal)} arquivo(s) -> principal | "
                f"{len(objetos_pa)} arquivo(s) -> PA | alvo desta execução: {alvo}")

    # Caches separados por dataset (não misturam metadata nem marcadores).
    cache_principal = PUBLISH_CACHE_DIR / "principal"
    cache_pa = PUBLISH_CACHE_DIR / "pa"

    # Migração única do cache antigo (layout achatado) -> principal/.
    # (só faz sentido quando o principal está no escopo desta execução)
    if alvo in ("ambos", "principal"):
        _migrar_cache_antigo_para_principal(cache_principal)

    # Publica o principal.
    if alvo in ("ambos", "principal"):
        _publicar_dataset(
            api, s3_client,
            qual="principal",
            dataset_slug=DATASET_PRINCIPAL_SLUG,
            titulo=DATASET_PRINCIPAL_TITULO,
            objetos_do_dataset=objetos_principal,
            cache_dir=cache_principal,
            kaggle_user=kaggle_user,
            forcar=forcar,
        )
    else:
        logger.info("Dataset principal fora do alvo desta execução -- pulando.")

    # Publica o PA (só se houver arquivos de PA no bucket).
    if alvo in ("ambos", "pa"):
        if objetos_pa:
            _publicar_dataset(
                api, s3_client,
                qual="pa",
                dataset_slug=DATASET_PA_SLUG,
                titulo=DATASET_PA_TITULO,
                objetos_do_dataset=objetos_pa,
                cache_dir=cache_pa,
                kaggle_user=kaggle_user,
                forcar=forcar,
            )
        else:
            logger.info("Nenhum arquivo de PA no bucket ainda -- dataset do PA não será criado nesta execução.")
    else:
        logger.info("Dataset do PA fora do alvo desta execução -- pulando.")


def _parse_args():
    import argparse
    p = argparse.ArgumentParser(
        description="Publica o Data Lake (MinIO) nos datasets Kaggle."
    )
    p.add_argument(
        "--dataset", choices=["principal", "pa", "ambos"], default="ambos",
        help="Qual dataset publicar: 'principal', 'pa' ou 'ambos' (padrão). "
             "Só sobe os arquivos que mudaram; se nada mudou, pula o envio.",
    )
    p.add_argument(
        "--forcar", action="store_true",
        help="Republica mesmo sem mudanças detectadas (ex.: se a última "
             "publicação ao Kaggle falhou mas o cache já está sincronizado).",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    load_lake_to_kaggle(alvo=args.dataset, forcar=args.forcar)
