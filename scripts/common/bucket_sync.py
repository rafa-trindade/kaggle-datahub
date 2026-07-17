"""
Verificação de novidade contra o BUCKET (MinIO/S3), não contra disco
local -- diferença estrutural chave deste projeto em relação ao
onco-360-foundation.

No onco-360-foundation, cada fetch_*.py comparava o que acabou de
baixar com o que já estava salvo em data/raw/ (persistente entre
execuções) pra decidir se valia a pena reprocessar. Aqui, como nada
fica local depois do upload (dados muito mais pesados, sem filtro
temático), essa comparação não tem mais uma base local confiável --
o bucket é que vira a fonte de verdade sobre "isso já existe?".

Fluxo típico de uso, num fetch_*.py ou process_*.py:

    from scripts.common.bucket_sync import already_in_bucket, upload_and_cleanup

    # antes de baixar/processar um arquivo:
    if already_in_bucket(s3_key, tamanho_local_esperado=tamanho_no_ftp):
        continue  # pula, já está lá com o mesmo tamanho

    ... baixa/processa pro caminho_local ...

    # depois de gerar o arquivo final local:
    upload_and_cleanup(caminho_local, s3_key)  # sobe e apaga o local

Comparação por TAMANHO primeiro (barato, um head_object só); só desce
pra comparação por HASH se os tamanhos baterem E o chamador pedir
verificação mais forte (arquivos pequenos o suficiente pra valer a
pena, como CSVs processados -- não vale a pena baixar de volta um
.dbc de 100MB só pra conferir hash).
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
    """ETag do objeto no bucket, sem baixar o conteúdo -- só é um MD5
    verdadeiro pra uploads simples (sem multipart); usar como sinal
    adicional, não como garantia absoluta pra arquivos multipart."""
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
    """
    True se o bucket já tem esse arquivo, considerado equivalente ao
    que temos/vamos gerar localmente.

    - Se só `tamanho_local_esperado` for passado: compara por tamanho
      (rápido, um head_object só) -- suficiente pra arquivos grandes
      onde recalcular hash não compensa (ex: .dbc baixados do FTP, que
      já reportam tamanho confiável na origem).
    - Se `caminho_local_para_hash` também for passado (arquivo já
      existe localmente): compara por hash MD5 além do tamanho --
      mais seguro pra arquivos processados/gerados (onde o mesmo
      tamanho por coincidência é mais preocupante).
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
    """Sobe o arquivo pro bucket e apaga a cópia local (comportamento
    padrão deste projeto -- nada fica persistido em disco depois de
    publicado). Passe apagar_local=False só em cenários de
    depuração/teste."""
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
    """
    Manifesto de arquivos-fonte já incorporados ao último output
    consolidado publicado (nome do arquivo -> tamanho em bytes),
    guardado como um JSON pequeno dentro do próprio bucket.

    Usado quando VÁRIOS arquivos brutos (ex: um .dbc por ano) viram UM
    ÚNICO arquivo final consolidado (ex: um CSV com todos os anos) --
    não existe uma chave 1-pra-1 no bucket pra checar cada bruto
    individualmente (só o final consolidado é publicado). O manifesto
    resolve isso: antes de baixar/reprocessar um arquivo-fonte, checa
    se ele (mesmo nome, mesmo tamanho) já está registrado aqui.
    """
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