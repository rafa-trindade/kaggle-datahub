from scripts.process.dados_abertos.base_process_abertos import LANDING_DIR, RAW_DIR, query_para_csv

def main():
    landing_file = LANDING_DIR / "csv_cnes" / "cnes_estabelecimentos_raw.csv"
    csv_final = RAW_DIR / "dados_abertos" / "cnes_estabelecimentos_de_saude" / "raw_cnes_estabelecimentos.csv"

    if not landing_file.exists():
        print(f"Erro: Arquivo base não encontrado ({landing_file})")
        return

    query = f"""
        SELECT *
        FROM read_csv(
            '{landing_file}',
            delim=';',
            header=true,
            encoding='ISO_8859_1',
            all_varchar=true,
            ignore_errors=true
        )
    """
    query_para_csv(query, csv_final)

if __name__ == "__main__":
    main()