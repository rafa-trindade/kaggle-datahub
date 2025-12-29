import os
import requests
import zipfile
import duckdb
import tempfile
from io import BytesIO

# -----------------------------
# Paths
# -----------------------------
file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(file_path)

scripts_dir = os.path.dirname(os.path.dirname(current_dir))
base_dir = os.path.dirname(scripts_dir)

parquet_dir = os.path.join(base_dir, "data", "raw")
os.makedirs(parquet_dir, exist_ok=True)

parquet_file = os.path.join(
    parquet_dir,
    "raw_cnes_estabelecimentos.parquet"
)

# -----------------------------
# Download
# -----------------------------
url = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/cnes_estabelecimentos_csv.zip"

print("Baixando o arquivo...")
response = requests.get(url)
response.raise_for_status()

# -----------------------------
# Unzip (igual ao pandas)
# -----------------------------
print("Descompactando o arquivo...")
with zipfile.ZipFile(BytesIO(response.content)) as z:
    csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]

    with z.open(csv_name) as csvfile:
        # DuckDB precisa de caminho → temp file
        with tempfile.NamedTemporaryFile(
            suffix=".csv",
            delete=False
        ) as tmp:
            tmp.write(csvfile.read())
            tmp_path = tmp.name

# -----------------------------
# DuckDB processa
# -----------------------------
print(f"Lendo {csv_name} com DuckDB...")

con = duckdb.connect()

con.execute(f"""
    COPY (
        SELECT *
        FROM read_csv(
            '{tmp_path}',
            delim=';',
            header=true,
            encoding='ISO_8859_1',
            all_varchar=true,
            ignore_errors=true
        )
    )
    TO '{parquet_file}'
    (FORMAT PARQUET);
""")

# -----------------------------
# Cleanup
# -----------------------------
os.remove(tmp_path)

print(f"Salvando em {parquet_file}...")
print("Concluído!")
