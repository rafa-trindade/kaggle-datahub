"""
Gera metadados.csv na raiz do bucket/dataset -- manifesto de todos os
arquivos publicados, com fonte(s) relacionada(s), tamanho, contagem de
registros (parquet, lida só do rodapé/metadata do arquivo -- sem
baixar o arquivo inteiro, importante dado que SIH/SIM têm parquets de
vários GB) e data de modificação.

Diferente do onco-360-foundation (que compara contra uma lista fixa de
"arquivos_saida" esperados por Fonte, cruzando com data/raw/ local):
aqui listamos o BUCKET REAL primeiro. Duas razões pra essa escolha:

  1. Este projeto não persiste nada localmente entre execuções (ver
     scripts/config/fontes.py) -- não existe um "data/raw/" pra
     escanear como no onco-360.
  2. Fontes como o SINAN produzem 58 arquivos dinamicamente (um por
     agravo, ver scripts/config/agravos_sinan.py) -- não dá pra manter
     uma lista de 58 nomes de arquivo no registro central sem
     duplicar informação que já vive em agravos_sinan.py.

A granularidade de "fonte" no manifesto fica no nível de PASTA
(pasta_bucket), já que várias Fontes do registro compartilham a mesma
pasta (ex: as 10 fontes do SIM todas têm pasta_bucket="sim").
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
    """Agrupa as Fontes do registro por pasta_bucket -- várias Fontes
    costumam cair na mesma pasta (ex: SIM inteiro em pasta_bucket='sim')."""
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
    """Lê só o rodapé/metadata do parquet (não baixa o arquivo inteiro
    -- importante pros parquets de vários GB do SIH/SIM)."""
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

    # Salva em data/metadados.csv -- PERSISTENTE, fica no repositório
    # (diferente de tudo mais em data/, que é efêmero) -- serve de
    # referência rápida do que está no dataset sem precisar acessar o
    # bucket ou o Kaggle.
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