"""Gera metadados.csv: manifesto de arquivos publicados com metadados.

Lê metadata do parquet (não baixa arquivo inteiro). Agrupa por pasta_bucket.
"""
import csv
from collections import defaultdict

import pyarrow.parquet as pq
import pyarrow.fs as pafs

from scripts.common.paths import LANDING_DIR, DATA_DIR
from scripts.common import env
from scripts.common.bucket_sync import get_s3_client
from scripts.config.fontes import FONTES

NOME_ARQUIVO_SAIDA = "datahub-metadados.csv"
CAMINHO_LOCAL_PERSISTENTE = DATA_DIR / NOME_ARQUIVO_SAIDA  # fica no repositório, não é apagado
COLUNAS = ["arquivo", "pasta", "fontes_relacionadas", "tamanho_bytes", "num_registros", "data_modificacao"]


def _descricao_por_pasta() -> dict[str, str]:
    """Agrupa Fontes por pasta_bucket."""
    agrupado = defaultdict(list)
    for f in FONTES:
        agrupado[f.pasta_bucket].append(f.nome)
    return {pasta: " | ".join(nomes) for pasta, nomes in agrupado.items()}


def _montar_s3_filesystem() -> pafs.S3FileSystem:
    endpoint_sem_protocolo = env.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    esquema = "https" if env.MINIO_ENDPOINT.startswith("https://") else "http"
    return pafs.S3FileSystem(
        endpoint_override=endpoint_sem_protocolo,
        access_key=env.MINIO_ROOT_USER,
        secret_key=env.MINIO_ROOT_PASSWORD,
        scheme=esquema,
    )


def _contar_registros_parquet(s3_fs: pafs.S3FileSystem, bucket: str, key: str) -> int | None:
    """Lê metadata do parquet (não baixa arquivo inteiro)."""
    try:
        caminho_s3 = f"{bucket}/{key}"
        pf = pq.ParquetFile(caminho_s3, filesystem=s3_fs)
        return pf.metadata.num_rows
    except Exception as e:
        print(f"[AVISO] Não consegui contar registros de {key}: {e}")
        return None


def gerar_linhas(s3_client, s3_fs, bucket: str, descricoes: dict[str, str]) -> list[dict]:
    paginator = s3_client.get_paginator("list_objects_v2")
    linhas = []

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            nome_arquivo = key.rsplit("/", 1)[-1]
            if nome_arquivo == "_manifest.json" or nome_arquivo == NOME_ARQUIVO_SAIDA:
                continue

            pasta = key.split("/")[0] if "/" in key else ""

            num_registros = None
            if key.endswith(".parquet"):
                num_registros = _contar_registros_parquet(s3_fs, bucket, key)

            linhas.append({
                "arquivo": key,
                "pasta": pasta,
                "fontes_relacionadas": descricoes.get(pasta, "(não mapeado no registro)"),
                "tamanho_bytes": obj["Size"],
                "num_registros": num_registros,
                "data_modificacao": obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S"),
            })

    linhas.sort(key=lambda r: r["arquivo"])
    return linhas


def main():
    descricoes = _descricao_por_pasta()
    s3_client = get_s3_client()
    s3_fs = _montar_s3_filesystem()

    print(f"Listando bucket {env.MINIO_BUCKET} e contando registros dos parquets...")
    linhas = gerar_linhas(s3_client, s3_fs, env.MINIO_BUCKET, descricoes)

    # Salva localmente (persistente) e no bucket
    with open(CAMINHO_LOCAL_PERSISTENTE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLUNAS)
        writer.writeheader()
        writer.writerows(linhas)

    s3_client.upload_file(str(CAMINHO_LOCAL_PERSISTENTE), env.MINIO_BUCKET, NOME_ARQUIVO_SAIDA)

    nao_mapeados = [l["pasta"] for l in linhas if l["fontes_relacionadas"] == "(não mapeado no registro)"]
    if nao_mapeados:
        print(f"[AVISO] Pasta(s) sem Fonte correspondente no registro: {sorted(set(nao_mapeados))}")

    print(f"✔ {NOME_ARQUIVO_SAIDA} salvo em {CAMINHO_LOCAL_PERSISTENTE} e publicado na raiz do bucket "
          f"({len(linhas)} arquivo(s) catalogados).")


if __name__ == "__main__":
    main()