"""IBGE - População Estimada por Município (process).

Mesclagem por ano (remove antiga antes de adicionar nova).
"""
import json
import duckdb
import pandas as pd

from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes
from scripts.common.bucket_sync import carregar_manifesto, salvar_manifesto, get_s3_client, upload_and_cleanup
from scripts.common import env

JSON_DIR = LANDING_DIR / "json_ibge_populacao"
PASTA_BUCKET = "ibge"
NOME_ARQUIVO_FINAL = "populacao_estimada.parquet"


def parse_um_ano(caminho_json, ano: int) -> pd.DataFrame:
    """Carrega JSON de um ano e retorna DataFrame normalizado."""
    dados = json.loads(caminho_json.read_text(encoding="utf-8"))
    linhas = dados[1:]  # primeiro elemento é o cabeçalho de rótulos, não dado

    registros = []
    for linha in linhas:
        registros.append({
            "COD_MUNICIPIO": linha.get("D1C"),
            "NOME_MUNICIPIO": linha.get("D1N"),
            "ANO": ano,
            "POPULACAO_ESTIMADA": linha.get("V"),
        })
    return pd.DataFrame(registros)


def main():
    if not JSON_DIR.exists():
        print(f"[INFO] {JSON_DIR} não existe -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    arquivos_json = sorted(JSON_DIR.glob("populacao_*.json"))
    if not arquivos_json:
        print("[INFO] Nenhum JSON novo/alterado -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    anos_novos = set()
    partes = []
    for caminho in arquivos_json:
        ano = int(caminho.stem.replace("populacao_", ""))
        anos_novos.add(ano)
        partes.append(parse_um_ano(caminho, ano))

    df_novos = pd.concat(partes, ignore_index=True)
    print(f"Anos novos/alterados processados: {sorted(anos_novos)} ({len(df_novos)} linhas no total).")

    s3_key = f"{PASTA_BUCKET}/{NOME_ARQUIVO_FINAL}"
    s3 = get_s3_client()
    caminho_existente_temp = JSON_DIR / "_existente_temp.parquet"
    caminho_novos_temp = JSON_DIR / "_novos_temp.parquet"
    caminho_final_temp = JSON_DIR / NOME_ARQUIVO_FINAL

    df_novos.to_parquet(caminho_novos_temp, index=False)

    tem_existente = False
    try:
        s3.download_file(env.MINIO_BUCKET, s3_key, str(caminho_existente_temp))
        tem_existente = True
        print(f"Parquet já publicado encontrado -- mesclando (removendo anos {sorted(anos_novos)} da versão antiga antes de adicionar a nova).")
    except Exception:
        print("Nada publicado ainda -- esta é a primeira publicação.")

    con = duckdb.connect()
    if tem_existente:
        anos_lista = ", ".join(str(a) for a in anos_novos)
        query = f"""
            COPY (
                SELECT * FROM read_parquet('{caminho_existente_temp}')
                WHERE ANO NOT IN ({anos_lista})
                UNION ALL BY NAME
                SELECT * FROM read_parquet('{caminho_novos_temp}')
            ) TO '{caminho_final_temp}' (FORMAT PARQUET);
        """
    else:
        query = f"COPY (SELECT * FROM read_parquet('{caminho_novos_temp}')) TO '{caminho_final_temp}' (FORMAT PARQUET);"
    con.execute(query)
    contagem = con.execute(f"SELECT COUNT(*) FROM read_parquet('{caminho_final_temp}')").fetchone()[0]
    con.close()
    print(f"✔ {contagem} registros no Parquet final mesclado.")

    caminho_novos_temp.unlink(missing_ok=True)
    if tem_existente:
        caminho_existente_temp.unlink(missing_ok=True)

    sucesso = upload_and_cleanup(caminho_final_temp, s3_key)
    if not sucesso:
        return exit_codes.ERRO

    manifesto = carregar_manifesto(PASTA_BUCKET)
    for caminho in arquivos_json:
        manifesto[caminho.name] = caminho.stat().st_size
        caminho.unlink()
    salvar_manifesto(PASTA_BUCKET, manifesto)

    return exit_codes.SUCESSO


if __name__ == "__main__":
    exit(main())