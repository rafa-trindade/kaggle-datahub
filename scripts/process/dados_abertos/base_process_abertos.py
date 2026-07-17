import duckdb
from pathlib import Path

from scripts.common.paths import BASE_DIR, RAW_DIR, LANDING_DIR  # noqa: F401

def query_para_parquet(query: str, caminho_parquet: Path, con=None):
    """Executa uma query no DuckDB e exporta o resultado pra Parquet --
    formato padrão pros dados principais deste projeto (CSV fica
    reservado pra arquivos auxiliares, como metadados)."""
    fechar_conexao = False
    if con is None:
        con = duckdb.connect()
        fechar_conexao = True

    print(f"Processando e salvando dados em: {caminho_parquet.name} ...")
    caminho_parquet.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
            {query}
        ) TO '{caminho_parquet}' (FORMAT PARQUET);
    """)
    print("✔ Arquivo Parquet gerado com sucesso!")

    if fechar_conexao:
        con.close()


def query_para_csv(query: str, caminho_csv: Path, con=None):
    """
    Executa uma query no DuckDB e exporta o resultado diretamente para CSV.
    Uso reservado a arquivos auxiliares (ex: metadados) -- dados
    principais devem usar query_para_parquet.
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