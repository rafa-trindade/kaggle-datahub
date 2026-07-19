"""CNES - Estabelecimentos de Saúde (extract).

Cadastro completo de estabelecimentos de saúde do Brasil.
"""
import requests

from scripts.extract.dados_abertos.base_dados_abertos import (
    verificar_novidade_http, baixar_e_extrair_csv, LANDING_DIR,
)
from scripts.common import exit_codes

URL = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/cnes_estabelecimentos_csv.zip"
CSV_DIR = LANDING_DIR / "csv_cnes"
PASTA_BUCKET = "cnes"
CHAVE_MANIFESTO = "estabelecimentos_de_saude.csv"

if __name__ == "__main__":
    try:
        houve_novidade, tamanho_remoto = verificar_novidade_http(URL, PASTA_BUCKET, CHAVE_MANIFESTO)
    except requests.RequestException as e:
        print(f"[ERRO] Falha ao checar novidade: {e}")
        exit(exit_codes.ERRO)

    if not houve_novidade:
        print("[SKIP] Arquivo sem mudança desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    landing_file = CSV_DIR / "cnes_estabelecimentos_raw.csv"
    baixar_e_extrair_csv(URL, landing_file)

    exit(exit_codes.SUCESSO)