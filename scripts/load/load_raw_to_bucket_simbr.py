import os
from datetime import datetime
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

try:
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

BASE_DIR = "/opt/airflow/data"
RAW_DIR = os.path.join(BASE_DIR, "raw")
METADADOS_FILE = os.path.join(RAW_DIR, "raw_metadados_simbr.csv")

ARQUIVOS_PARA_ENVIAR = {
    "raw_sistema_info_mortalidade_prelim.parquet": "raw_info_mortalidade_prelim.parquet",
    "raw_sistema_info_mortalidade.parquet": "raw_info_mortalidade.parquet",
    "raw_macroregiao_de_saude.parquet": "raw_macroregiao.parquet",
    "raw_metadados_simbr.csv": "raw_metadados_simbr.csv"
}

load_dotenv()

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BUCKET = "sim-br"

# ----------------------------
# Funções utilitárias
# ----------------------------
def bytes_to_mib(size_bytes: int) -> float:
    if size_bytes is None:
        return 0.0
    return round(size_bytes / (1024 ** 2), 2)

def contar_linhas_parquet(caminho: str) -> int:
    if HAS_PYARROW:
        parquet_file = pq.ParquetFile(caminho)
        return parquet_file.metadata.num_rows
    else:
        df = pd.read_parquet(caminho)
        return len(df)

# ----------------------------
# Coleta de metadados
# ----------------------------
def coletar_metadados_raw():
    if not os.path.exists(RAW_DIR):
        print(f"Pasta '{RAW_DIR}' não encontrada. Nada para processar.")
        return None, pd.DataFrame()

    arquivos = [
        f for f in ARQUIVOS_PARA_ENVIAR.keys()
        if os.path.isfile(os.path.join(RAW_DIR, f)) and f != os.path.basename(METADADOS_FILE)
    ]

    if not arquivos:
        print("Nenhum arquivo específico encontrado para processar.")
        return None, pd.DataFrame()

    data_extracao = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if os.path.exists(METADADOS_FILE):
        try:
            df_existente = pd.read_csv(METADADOS_FILE)
        except Exception as e:
            print(f"Erro ao ler metadados existentes, sobrescrevendo arquivo: {e}")
            df_existente = pd.DataFrame(columns=["data_extracao", "arquivo", "numero_registros", "tamanho_mib"])
    else:
        df_existente = pd.DataFrame(columns=["data_extracao", "arquivo", "numero_registros", "tamanho_mib"])

    registros_para_adicionar = []

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(RAW_DIR, arquivo)
        try:
            tamanho_bytes = os.path.getsize(caminho_arquivo)
            tamanho_mib = bytes_to_mib(tamanho_bytes)

            num_registros = contar_linhas_parquet(caminho_arquivo) if arquivo.lower().endswith(".parquet") else pd.read_csv(caminho_arquivo).shape[0]

            df_arquivo = df_existente[df_existente["arquivo"] == arquivo]
            existe_mesmo = any(
                (df_arquivo["numero_registros"] == num_registros) &
                (df_arquivo["tamanho_mib"] == tamanho_mib)
            )

            if not existe_mesmo:
                registros_para_adicionar.append({
                    "data_extracao": data_extracao,
                    "arquivo": arquivo,
                    "numero_registros": num_registros,
                    "tamanho_mib": tamanho_mib,
                })
                print(f"Coletado metadado de '{arquivo}': {num_registros} registros, {tamanho_mib} MiB")
            else:
                print(f"Nenhuma alteração para '{arquivo}', não adicionando registro.")

        except Exception as e:
            print(f"Erro ao processar '{caminho_arquivo}': {e}")

    if not registros_para_adicionar and df_existente.empty:
        registros_para_adicionar = []
        for arquivo in arquivos:
            caminho_arquivo = os.path.join(RAW_DIR, arquivo)
            tamanho_mib = bytes_to_mib(os.path.getsize(caminho_arquivo))
            num_registros = contar_linhas_parquet(caminho_arquivo) if arquivo.lower().endswith(".parquet") else pd.read_csv(caminho_arquivo).shape[0]
            registros_para_adicionar.append({
                "data_extracao": data_extracao,
                "arquivo": arquivo,
                "numero_registros": num_registros,
                "tamanho_mib": tamanho_mib,
            })

    if registros_para_adicionar:
        df_final = pd.concat([df_existente, pd.DataFrame(registros_para_adicionar)], ignore_index=True)
        df_final.to_csv(METADADOS_FILE, index=False)
        print(f"Metadados atualizados em '{METADADOS_FILE}'.")

        registros_para_adicionar.append({
            "data_extracao": data_extracao,
            "arquivo": os.path.basename(METADADOS_FILE),
            "numero_registros": pd.read_csv(METADADOS_FILE).shape[0],
            "tamanho_mib": bytes_to_mib(os.path.getsize(METADADOS_FILE))
        })

    return METADADOS_FILE, pd.DataFrame(registros_para_adicionar)

# ----------------------------
# S3 / MinIO
# ----------------------------
def criar_s3_client():
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        print(f"Bucket '{MINIO_BUCKET}' não encontrado. Criando...")
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
    return s3_client

def enviar_pasta_raw_para_minio(df_novos_metadados=None) -> bool:
    """Envia somente os arquivos específicos, renomeando no bucket"""
    if not os.path.exists(RAW_DIR):
        print(f"Pasta '{RAW_DIR}' não encontrada. Nada para enviar.")
        return False

    if df_novos_metadados is None or df_novos_metadados.empty:
        print("Nenhum arquivo alterado para enviar.")
        return False

    s3_client = criar_s3_client()
    enviados = False

    for _, row in df_novos_metadados.iterrows():
        arquivo_local = row["arquivo"]
        if arquivo_local not in ARQUIVOS_PARA_ENVIAR:
            continue  # só envia arquivos mapeados

        caminho_arquivo = os.path.join(RAW_DIR, arquivo_local)
        arquivo_bucket = ARQUIVOS_PARA_ENVIAR[arquivo_local]

        if not os.path.exists(caminho_arquivo):
            print(f"[WARN] Arquivo '{arquivo_local}' não encontrado, pulando...")
            continue

        try:
            s3_client.upload_file(caminho_arquivo, MINIO_BUCKET, arquivo_bucket)
            print(f"Arquivo '{arquivo_local}' enviado para MinIO no bucket '{MINIO_BUCKET}' como '{arquivo_bucket}'.")
            enviados = True
        except NoCredentialsError:
            print("Erro: credenciais do MinIO inválidas.")
            break
        except Exception as e:
            print(f"Erro ao enviar '{arquivo_local}': {e}")

    return enviados

# ----------------------------
# Main
# ----------------------------
def main() -> bool:
    _, df_novos = coletar_metadados_raw()
    enviados = enviar_pasta_raw_para_minio(df_novos)
    if enviados:
        print("[INFO] Arquivos enviados ao bucket.")
    else:
        print("[INFO] Nenhum arquivo novo ou alterado para enviar.")
    return enviados

if __name__ == "__main__":
    updated = main()
    exit(0 if updated else 1)
