from scripts.extract.dados_abertos.base_dados_abertos import LANDING_DIR, baixar_e_extrair_csv

CSV_DIR = LANDING_DIR / "csv_macroregiao"

def main():
    url = "https://arquivosdadosabertos.saude.gov.br/dados/dbgeral/macroregiao_de_saude.zip"
    landing_file = CSV_DIR / "macroregiao_de_saude_raw.csv"
    
    baixar_e_extrair_csv(url, landing_file)
    print("Lembre-se de garantir que o arquivo 'macro_geolocalizacao.xls' está na pasta Landing.")

if __name__ == "__main__":
    main()