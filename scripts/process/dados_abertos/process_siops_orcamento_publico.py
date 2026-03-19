from scripts.process.dados_abertos.base_process_abertos import LANDING_DIR, RAW_DIR, query_para_csv

def consolidar_pasta_siops(nome_pasta: str, nome_arquivo_saida: str, drop_municipio: bool = False):
    pasta_input = LANDING_DIR / nome_pasta
    csv_final = RAW_DIR / "dados_abertos" / "siops_orcamento_publico" /  nome_arquivo_saida

    arquivos = list(pasta_input.glob("*.csv"))
    if not arquivos:
        print(f"Aviso: Nenhum arquivo encontrado em {pasta_input}")
        return

    padrao_leitura = str(pasta_input / "*.csv")
    
    select_clause = "SELECT * EXCLUDE (municipio)" if drop_municipio else "SELECT *"

    query = f"""
        {select_clause}
        FROM read_csv_auto(
            '{padrao_leitura}',
            union_by_name=true,
            files_to_sniff=-1
        )
    """
    
    query_para_csv(query, csv_final)

def main():
    print("Consolidando SIOPS Subfunção...")
    consolidar_pasta_siops("csv_siops_subfuncao", "raw_siops_exec_saude.csv", drop_municipio=True)

    print("\nConsolidando SIOPS RREO...")
    consolidar_pasta_siops("csv_siops_rreo", "raw_siops_exec_rreo.csv", drop_municipio=True)

    print("\nConsolidando SIOPS Indicadores...")
    consolidar_pasta_siops("csv_siops_indicador", "raw_siops_indicadores.csv", drop_municipio=False)

if __name__ == "__main__":
    main()