import os
import logging
from pathlib import Path
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------- Caminhos e Diretórios -------------------
CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = CURRENT_DIR.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

# Carrega as variáveis do .env localizado na raiz do projeto
load_dotenv(BASE_DIR / ".env")

# -----------------------------
# Configurações MinIO
# -----------------------------
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET")

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
    # Verifica se o bucket existe, se não, cria
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        logger.info(f"Bucket '{MINIO_BUCKET}' não encontrado. Criando...")
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
    return s3_client

def enviar_raw_para_minio() -> bool:
    if not RAW_DIR.exists():
        logger.warning(f"Pasta '{RAW_DIR}' não encontrada. Nada para enviar.")
        return False

    # Pega todos os arquivos recursivamente, ignorando os arquivos .gitkeep
    arquivos = [f for f in RAW_DIR.rglob("*") if f.is_file() and f.name != ".gitkeep"]
    
    if not arquivos:
        logger.warning("Nenhum arquivo encontrado para upload na pasta raw.")
        return False

    s3_client = criar_s3_client()
    sucesso_total = True
    algum_enviado = False

    logger.info(f"Iniciando verificação de {len(arquivos)} arquivo(s)...")

    for caminho_local in arquivos:
        # Pega o caminho relativo à pasta raw. Ex: datasus/arquivo.csv
        s3_key = str(caminho_local.relative_to(RAW_DIR)).replace("\\", "/")
        tamanho_local = caminho_local.stat().st_size
        
        # Verifica direto no MinIO se o arquivo já existe e tem o mesmo tamanho
        precisa_enviar = True
        try:
            response = s3_client.head_object(Bucket=MINIO_BUCKET, Key=s3_key)
            tamanho_remoto = response.get('ContentLength')
            
            if tamanho_local == tamanho_remoto:
                logger.info(f"[SKIP] {s3_key} (já existe no bucket)")
                precisa_enviar = False
        except ClientError as e:
            # Se der erro 404, significa que o arquivo não existe, então enviamos
            if e.response['Error']['Code'] != '404':
                logger.error(f"Erro ao consultar '{s3_key}' no MinIO: {e}")
        
        # Faz o upload se for arquivo novo ou se o tamanho estiver diferente
        if precisa_enviar:
            try:
                logger.info(f"[UPLOAD] Enviando {s3_key} ...")
                s3_client.upload_file(str(caminho_local), MINIO_BUCKET, s3_key)
                logger.info(f"[UPLOAD OK] {s3_key}")
                algum_enviado = True
            except NoCredentialsError:
                logger.error("Erro: Credenciais do MinIO inválidas ou ausentes no .env")
                return False
            except Exception as e:
                logger.error(f"[ERRO UPLOAD] '{s3_key}': {e}")
                sucesso_total = False

    return algum_enviado and sucesso_total

# ----------------------------
# Main
# ----------------------------

def main() -> bool:
    logger.info("Iniciando upload direto para o MinIO...")
    enviados = enviar_raw_para_minio()
    
    if enviados:
        logger.info("Uploads concluídos com sucesso.")
    else:
        logger.info("Nenhum arquivo novo precisou ser enviado.")
        
    return enviados

if __name__ == "__main__":
    updated = main()
    exit(0 if updated else 1)