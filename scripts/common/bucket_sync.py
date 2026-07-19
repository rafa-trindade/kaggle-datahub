"""
Verificação de novidade contra o BUCKET (MinIO/S3), não contra disco local.
O bucket é a fonte de verdade sobre "arquivo já existe".

Uso básico:
  - Checar se existe: already_in_bucket(s3_key, tamanho_esperado)
  - Upload e cleanup: upload_and_cleanup(caminho_local, s3_key)
"""
import hashlib
import json
import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from scripts.common import env

logger = logging.getLogger(__name__)

_client = None


def get_s3_client():
    """Client S3 compartilhado (lazy, uma única conexão reaproveitada
    em todas as chamadas do processo)."""
    global _client
    if _client is None:
        _client = boto3.client(
            "s3",
            endpoint_url=env.MINIO_ENDPOINT,
            aws_access_key_id=env.MINIO_ROOT_USER,
            aws_secret_access_key=env.MINIO_ROOT_PASSWORD,
        )
        try:
            _client.head_bucket(Bucket=env.MINIO_BUCKET)
        except ClientError:
            logger.info(f"Bucket '{env.MINIO_BUCKET}' não encontrado. Criando...")
            _client.create_bucket(Bucket=env.MINIO_BUCKET)
    return _client


def _tamanho_remoto(s3_key: str) -> int | None:
    """Tamanho do objeto no bucket, ou None se não existir."""
    s3 = get_s3_client()
    try:
        resposta = s3.head_object(Bucket=env.MINIO_BUCKET, Key=s3_key)
        return resposta["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return None
        raise


def _hash_remoto_md5(s3_key: str) -> str | None:
    """ETag do objeto (MD5 confiável apenas para uploads simples, não multipart)."""
    s3 = get_s3_client()
    try:
        resposta = s3.head_object(Bucket=env.MINIO_BUCKET, Key=s3_key)
        return resposta["ETag"].strip('"')
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return None
        raise


def _hash_local_md5(caminho: Path) -> str:
    md5 = hashlib.md5()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(8 * 1024 * 1024), b""):
            md5.update(bloco)
    return md5.hexdigest()


def already_in_bucket(s3_key: str, tamanho_local_esperado: int | None = None,
                       caminho_local_para_hash: Path | None = None) -> bool:
    """True se bucket tem esse arquivo com tamanho/hash equivalente.
        
    Compara por tamanho (rápido) e opcionalmente por MD5 se o arquivo
    local for fornecido (mais seguro para arquivos processados).
    """
    tamanho_remoto = _tamanho_remoto(s3_key)
    if tamanho_remoto is None:
        return False

    if tamanho_local_esperado is not None and tamanho_remoto != tamanho_local_esperado:
        return False

    if caminho_local_para_hash is not None and caminho_local_para_hash.exists():
        hash_remoto = _hash_remoto_md5(s3_key)
        hash_local = _hash_local_md5(caminho_local_para_hash)
        if hash_remoto != hash_local:
            return False

    return True


def upload_and_cleanup(caminho_local: Path, s3_key: str, apagar_local: bool = True) -> bool:
    """Sobe para bucket e apaga local. Padrão do projeto: nada fica em disco."""
    s3 = get_s3_client()
    logger.info(f"[UPLOAD] Enviando {s3_key} ...")
    try:
        s3.upload_file(str(caminho_local), env.MINIO_BUCKET, s3_key)
        logger.info(f"[UPLOAD OK] {s3_key}")
    except Exception as e:
        logger.error(f"[ERRO UPLOAD] '{s3_key}': {e}")
        return False

    if apagar_local:
        try:
            caminho_local.unlink()
        except FileNotFoundError:
            pass

    return True


def _chave_manifesto(pasta_bucket: str) -> str:
    return f"{pasta_bucket}/_manifest.json"


def carregar_manifesto(pasta_bucket: str) -> dict[str, int]:
    """Carrega manifesto JSON: registro de arquivos-fonte já incorporados
    no último output consolidado publicado (nome -> tamanho).

    Usado quando múltiplos brutos viram um único consolidado."""
    s3 = get_s3_client()
    chave = _chave_manifesto(pasta_bucket)
    try:
        resposta = s3.get_object(Bucket=env.MINIO_BUCKET, Key=chave)
        return json.loads(resposta["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return {}
        raise


def salvar_manifesto(pasta_bucket: str, manifesto: dict[str, int]):
    """Regrava o manifesto -- chamar depois de publicar com sucesso um
    novo output consolidado, com o conjunto atualizado de arquivos-
    fonte incorporados."""
    s3 = get_s3_client()
    chave = _chave_manifesto(pasta_bucket)
    s3.put_object(
        Bucket=env.MINIO_BUCKET,
        Key=chave,
        Body=json.dumps(manifesto, indent=2, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"[MANIFESTO] {chave} atualizado ({len(manifesto)} arquivo(s) registrados).")