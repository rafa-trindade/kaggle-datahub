'''
Obtém lista de municípios com as informações de macrorregião e região de saúde
'''
import os
import requests
import zipfile
import pandas as pd
from io import BytesIO

file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(file_path)

scripts_dir = os.path.dirname(os.path.dirname(current_dir))
base_dir = os.path.dirname(scripts_dir)

parquet_dir = os.path.join(base_dir, "data", "raw")

os.makedirs(parquet_dir, exist_ok=True)

url = "https://arquivosdadosabertos.saude.gov.br/dados/dbgeral/macroregiao_de_saude.zip"

parquet_file = os.path.join(parquet_dir, "raw_macroregiao_de_saude.parquet")

print("Baixando o arquivo...")
response = requests.get(url)
response.raise_for_status() 

print("Descompactando o arquivo...")
with zipfile.ZipFile(BytesIO(response.content)) as z:
    csv_name = [name for name in z.namelist() if name.endswith(".csv")][0]
    with z.open(csv_name) as csvfile:
        print(f"Lendo {csv_name}...")
        df = pd.read_csv(csvfile, sep=";", encoding="utf-8-sig", dtype=str)
        
print(f"Salvando em {parquet_file}...")
df.to_parquet(parquet_file, index=False)
print("Concluído!")
