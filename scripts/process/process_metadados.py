"""Gera metadados.csv: manifesto de arquivos publicados com metadados.

Lê metadata do parquet (não baixa arquivo inteiro). Agrupa por pasta_bucket e
mapeia os arquivos para o ID exato da Fonte correspondente.
"""
import csv
import pyarrow.parquet as pq
import pyarrow.fs as pafs

from scripts.common.paths import DATA_DIR
from scripts.common import env
from scripts.common.bucket_sync import get_s3_client
from scripts.config.fontes import FONTES

NOME_ARQUIVO_SAIDA = "datahub-metadados.csv"
CAMINHO_LOCAL_PERSISTENTE = DATA_DIR / NOME_ARQUIVO_SAIDA

COLUNAS = ["arquivo", "diretorio", "descricao", "tamanho_bytes", "num_registros", "ultima_atualizacao"]

DESCRICOES_POR_ID = {f.id: f.descricao for f in FONTES}

MAPEAMENTO_ARQUIVO_ID = {
    # CNES
    "estabelecimentos_de_saude": "cnes_estabelecimentos",
    "habilitacoes": "cnes_habilitacoes",
    "leitos": "cnes_leitos",
    "profissionais": "cnes_profissionais",
    "equipamentos": "cnes_equipamentos",
    
    # SIM
    "causas_externas_cid10": "sim_causas_externas_cid10",
    "causas_externas_cid9": "sim_causas_externas_cid9",
    "fetais_cid10": "sim_dofet_cid10",
    "fetais_cid9": "sim_dofet_cid9",
    "infantis_cid10": "sim_doinf_cid10",
    "infantis_cid9": "sim_doinf_cid9",
    "maternos_cid10": "sim_domat",
    "residentes_exterior": "sim_dorext",
    "declaracoes_de_obito_cid10": "sim_do_cid10",
    "declaracoes_de_obito_cid9": "sim_do_cid9",
    
    # SINASC
    "nascido_vivo_exterior": "sinasc_dnex",
    "nascido_vivo": "sinasc",
    
    # SIH
    "aih_reduzida": "sih_rd",
    "aih_rejeitada": "sih_rj",
    "servicos_profissionais": "sih_sp",
    
    # IBGE e GEO
    "macroregiao_de_saude": "macroregiao",
    "populacao_estimada": "ibge_populacao",
    "pib_municipal": "ibge_pib_municipal",
    "pns_2013": "pns_2013",
    "pns_2019": "pns_2019",
}

def _obter_descricao_exata(pasta: str, nome_arquivo: str) -> str:
    """Acha a descrição da fonte cruzando o nome do arquivo com o ID da fonte."""
    
    for trecho, fonte_id in MAPEAMENTO_ARQUIVO_ID.items():
        if trecho in nome_arquivo:
            return DESCRICOES_POR_ID.get(fonte_id, "(descrição ausente no registro)")
            
    if pasta == "sinan":
        return DESCRICOES_POR_ID.get("sinan", "(descrição do sinan ausente)")
        
    return "(não mapeado no registro)"


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
    try:
        caminho_s3 = f"{bucket}/{key}"
        pf = pq.ParquetFile(caminho_s3, filesystem=s3_fs)
        return pf.metadata.num_rows
    except Exception as e:
        print(f"[AVISO] Não consegui contar registros de {key}: {e}")
        return None


def gerar_linhas(s3_client, s3_fs, bucket: str) -> list[dict]:
    paginator = s3_client.get_paginator("list_objects_v2")
    linhas = []

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            nome_arquivo = key.rsplit("/", 1)[-1]
            
            if nome_arquivo in ("_manifest.json", NOME_ARQUIVO_SAIDA):
                continue

            pasta = key.split("/")[0] if "/" in key else ""

            num_registros = None
            if key.endswith(".parquet"):
                num_registros = _contar_registros_parquet(s3_fs, bucket, key)

            linhas.append({
                "arquivo": key,
                "diretorio": pasta,
                "descricao": _obter_descricao_exata(pasta, nome_arquivo),  # A mágica do ID acontece aqui
                "tamanho_bytes": obj["Size"],
                "num_registros": num_registros,
                "ultima_atualizacao": obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S"),
            })

    linhas.sort(key=lambda r: r["arquivo"])
    return linhas


def main():
    s3_client = get_s3_client()
    s3_fs = _montar_s3_filesystem()

    print(f"Listando bucket {env.MINIO_BUCKET} e contando registros dos parquets...")
    linhas = gerar_linhas(s3_client, s3_fs, env.MINIO_BUCKET)

    with open(CAMINHO_LOCAL_PERSISTENTE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLUNAS)
        writer.writeheader()
        writer.writerows(linhas)

    s3_client.upload_file(str(CAMINHO_LOCAL_PERSISTENTE), env.MINIO_BUCKET, NOME_ARQUIVO_SAIDA)

    nao_mapeados = [l["diretorio"] for l in linhas if l["descricao"].startswith("(não mapeado")]
    if nao_mapeados:
        print(f"[AVISO] Pasta(s) sem Fonte correspondente no registro: {sorted(set(nao_mapeados))}")

    print(f"✔ {NOME_ARQUIVO_SAIDA} salvo em {CAMINHO_LOCAL_PERSISTENTE} e publicado na raiz do bucket "
          f"({len(linhas)} arquivo(s) catalogados).")


if __name__ == "__main__":
    main()