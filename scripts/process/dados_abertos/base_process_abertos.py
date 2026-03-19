import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
LANDING_DIR = BASE_DIR / "data" / "landing"

RAW_DIR.mkdir(parents=True, exist_ok=True)

def query_para_csv(query: str, caminho_csv: Path, con=None):
    """
    Executa uma query no DuckDB e exporta o resultado diretamente para CSV.
    """
    fechar_conexao = False
    if con is None:
        con = duckdb.connect()
        fechar_conexao = True
        
    print(f"Processando e salvando dados em: {caminho_csv.name} ...")
    con.execute(f"""
        COPY (
            {query}
        ) TO '{caminho_csv}' (HEADER, DELIMITER ',');
    """)
    print("✔ Arquivo CSV gerado com sucesso!")
    
    if fechar_conexao:
        con.close()