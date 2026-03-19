import pandas as pd
import duckdb
from scripts.process.dados_abertos.base_process_abertos import LANDING_DIR, RAW_DIR, query_para_csv

def main():
    landing_csv = LANDING_DIR / "csv_macroregiao" / "macroregiao_de_saude_raw.csv"
    geo_path = LANDING_DIR / "csv_macroregiao" / "macro_geolocalizacao.xls"
    csv_final = RAW_DIR / "dados_abertos" / "geo_macroregiao_de_saude" / "raw_geo_macroregiao_de_saude.csv"

    if not landing_csv.exists() or not geo_path.exists():
        print("Erro: Arquivos base (CSV ou XLS) não encontrados na Landing.")
        return

    print("Lendo CSV e XLS no Pandas para ajuste de zeros à esquerda...")
    df = pd.read_csv(landing_csv, sep=";", encoding="utf-8-sig", dtype=str)
    df_geo = pd.read_excel(geo_path, dtype=str)

    df["cod_municipio"] = df["cod_municipio"].str.zfill(6)
    df_geo["MUNCOD"] = df_geo["MUNCOD"].str.zfill(6)

    print("Fazendo merge no DuckDB...")
    con = duckdb.connect()
    con.register("macro", df)
    con.register("geo", df_geo)

    query = """
        SELECT m.*, g.*
        FROM macro m
        LEFT JOIN geo g
        ON m.cod_municipio = g.MUNCOD
    """
    
    query_para_csv(query, csv_final, con)
    con.close()

if __name__ == "__main__":
    main()