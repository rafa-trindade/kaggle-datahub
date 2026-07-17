"""
Macrorregião de Saúde (geolocalização) -- extract

Municípios brasileiros associados às suas macrorregiões de saúde,
regiões de saúde e coordenadas geográficas -- combina o CSV de
municípios (Dados Abertos da Saúde) com um arquivo complementar de
geolocalização.

Nota: precisa do arquivo 'macro_geolocalizacao.xls' colocado
MANUALMENTE em data/landing/csv_macroregiao/ -- sem isso, o process
não roda.
"""
import requests

from scripts.extract.dados_abertos.base_dados_abertos import (
    verificar_novidade_http, baixar_e_extrair_csv, LANDING_DIR,
)
from scripts.common import exit_codes

URL = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/dbgeral/macroregiao_de_saude_csv.zip"
CSV_DIR = LANDING_DIR / "csv_macroregiao"
PASTA_BUCKET = "geo"
CHAVE_MANIFESTO = "macroregiao_de_saude_raw.csv"

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
    landing_file = CSV_DIR / "macroregiao_de_saude_raw.csv"
    baixar_e_extrair_csv(URL, landing_file)

    geo_path = CSV_DIR / "macro_geolocalizacao.xls"
    if not geo_path.exists():
        print(f"[AVISO] '{geo_path.name}' não encontrado em {CSV_DIR} -- "
              f"coloque esse arquivo manualmente antes de rodar o process.")

    exit(exit_codes.SUCESSO)