"""Macrorregião de Saúde (geolocalização) -- process.

Merge CSV + XLS com ajuste de zeros à esquerda em códigos de município.
"""
import requests
import pandas as pd
import duckdb

from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes
from scripts.common.bucket_sync import carregar_manifesto, salvar_manifesto, upload_and_cleanup

CSV_DIR = LANDING_DIR / "csv_macroregiao"
LANDING_CSV = CSV_DIR / "macroregiao_de_saude_raw.csv"
GEO_PATH = CSV_DIR / "macro_geolocalizacao.xls"
PASTA_BUCKET = "geo"
CHAVE_MANIFESTO = "macroregiao_de_saude_raw.csv"
NOME_ARQUIVO_FINAL = "macroregiao_de_saude.parquet"
URL_ORIGEM = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/dbgeral/macroregiao_de_saude_csv.zip"


def main():
    if not LANDING_CSV.exists():
        print(f"[INFO] {LANDING_CSV} não existe -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    if not GEO_PATH.exists():
        print(f"[ERRO] '{GEO_PATH.name}' não encontrado -- coloque esse arquivo "
              f"manualmente em {CSV_DIR} antes de rodar.")
        return exit_codes.ERRO

    print("Lendo CSV e XLS no Pandas para ajuste de zeros à esquerda...")
    df = pd.read_csv(LANDING_CSV, sep=";", encoding="utf-8-sig", dtype=str)
    df_geo = pd.read_excel(GEO_PATH, dtype=str)

    df["cod_municipio"] = df["cod_municipio"].str.zfill(6)
    df_geo["MUNCOD"] = df_geo["MUNCOD"].str.zfill(6)

    print("Fazendo merge no DuckDB...")
    con = duckdb.connect()
    con.register("macro", df)
    con.register("geo", df_geo)

    caminho_parquet_temp = CSV_DIR / NOME_ARQUIVO_FINAL
    con.execute(f"""
        COPY (
            SELECT m.*, g.* EXCLUDE (MUNCOD)
            FROM macro m
            LEFT JOIN geo g ON m.cod_municipio = g.MUNCOD
        ) TO '{caminho_parquet_temp}' (FORMAT PARQUET);
    """)
    con.close()
    print("✔ Arquivo Parquet gerado com sucesso!")

    s3_key = f"{PASTA_BUCKET}/{NOME_ARQUIVO_FINAL}"
    sucesso = upload_and_cleanup(caminho_parquet_temp, s3_key)
    if not sucesso:
        return exit_codes.ERRO

    LANDING_CSV.unlink(missing_ok=True)
    # GEO_PATH não é apagado (arquivo manual, persistente)

    try:
        resposta = requests.head(URL_ORIGEM, timeout=30, allow_redirects=True)
        tamanho_atual = int(resposta.headers.get("Content-Length", 0))
        if tamanho_atual > 0:
            manifesto = carregar_manifesto(PASTA_BUCKET)
            manifesto[CHAVE_MANIFESTO] = tamanho_atual
            salvar_manifesto(PASTA_BUCKET, manifesto)
    except requests.RequestException as e:
        print(f"[AVISO] Publicado com sucesso, mas não consegui atualizar o manifesto: {e}")

    return exit_codes.SUCESSO


if __name__ == "__main__":
    exit(main())