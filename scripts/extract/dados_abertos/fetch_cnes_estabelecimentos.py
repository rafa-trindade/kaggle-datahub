from scripts.extract.dados_abertos.base_dados_abertos import LANDING_DIR, baixar_e_extrair_csv

CSV_DIR = LANDING_DIR / "csv_cnes"

def main():
    url = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/cnes_estabelecimentos_csv.zip"
    landing_file = CSV_DIR / "cnes_estabelecimentos_raw.csv"
    
    baixar_e_extrair_csv(url, landing_file)

if __name__ == "__main__":
    main()