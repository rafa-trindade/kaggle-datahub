"""
Obtém lista de municípios com as informações de macrorregião,
região de saúde e geolocalização
DuckDB usado apenas onde não quebra
"""

import os
import requests
import zipfile
import pandas as pd
import duckdb
from io import BytesIO

# --------------------------
# Caminhos base
# --------------------------
file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(file_path)

scripts_dir = os.path.dirname(os.path.dirname(current_dir))
base_dir = os.path.dirname(scripts_dir)

parquet_dir = os.path.join(base_dir, "data", "raw")
utils_dir = os.path.join(base_dir, "data", "utils")

os.makedirs(parquet_dir, exist_ok=True)

# --------------------------
# Baixar macroregiao (pandas – como estava)
# --------------------------
url = "https://arquivosdadosabertos.saude.gov.br/dados/dbgeral/macroregiao_de_saude.zip"
parquet_file = os.path.join(
    parquet_dir,
    "raw_macroregiao_de_saude.parquet"
)

print("Baixando o arquivo...")
response = requests.get(url)
response.raise_for_status()

print("Descompactando o arquivo...")
with zipfile.ZipFile(BytesIO(response.content)) as z:
    csv_name = [name for name in z.namelist() if name.endswith(".csv")][0]
    with z.open(csv_name) as csvfile:
        print(f"Lendo {csv_name}...")
        df = pd.read_csv(
            csvfile,
            sep=";",
            encoding="utf-8-sig",
            dtype=str
        )

# --------------------------
# Ler XLS (pandas – como estava)
# --------------------------
geo_path = os.path.join(utils_dir, "macro_geolocalizacao.xls")

print("Lendo arquivo de geolocalização...")
df_geo = pd.read_excel(geo_path, dtype=str)

# --------------------------
# Padronização (como estava)
# --------------------------
df["cod_municipio"] = df["cod_municipio"].str.zfill(6)
df_geo["MUNCOD"] = df_geo["MUNCOD"].str.zfill(6)

# --------------------------
# DuckDB entra AQUI (parte pesada)
# --------------------------
print("Fazendo merge com DuckDB...")

con = duckdb.connect()

con.register("macro", df)
con.register("geo", df_geo)

con.execute(f"""
    COPY (
        SELECT
            m.*,
            g.*
        FROM macro m
        LEFT JOIN geo g
        ON m.cod_municipio = g.MUNCOD
    )
    TO '{parquet_file}'
    (FORMAT PARQUET);
""")

print(f"Salvando em {parquet_file}...")
print("Concluído!")
