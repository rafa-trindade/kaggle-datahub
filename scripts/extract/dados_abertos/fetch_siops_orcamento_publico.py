import os
import time
import requests
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime

from scripts.extract.dados_abertos.base_dados_abertos import RAW_DIR, LANDING_DIR

# ------------------------------
# Configurações gerais
# ------------------------------
BASE_URL = "https://siops-consulta-publica-api.saude.gov.br/v1"

CSV_DIRS = {
    "siops_subfuncao": LANDING_DIR / "csv_siops_subfuncao",
    "siops_rro": LANDING_DIR / "csv_siops_rreo",
    "siops_indicador": LANDING_DIR / "csv_siops_indicador"
}

PARQUET_DIR = RAW_DIR
PARQUET_FILES = {
    "siops_subfuncao": "raw_siops_exec_saude.parquet",
    "siops_rro": "raw_siops_exec_rreo.parquet",
    "siops_indicador": "raw_siops_indicadores.parquet"
}

# Códigos IBGE das UFs
UFS = {
    "RO": 11, "AC": 12, "AM": 13, "RR": 14, "PA": 15, "AP": 16, "TO": 17,
    "MA": 21, "PI": 22, "CE": 23, "RN": 24, "PB": 25, "PE": 26, "AL": 27,
    "SE": 28, "BA": 29, "MG": 31, "ES": 32, "RJ": 33, "SP": 35, "PR": 41,
    "SC": 42, "RS": 43, "MS": 50, "MT": 51, "GO": 52, "DF": 53
}

ano_atual = datetime.now().year

ANOS_SUBFUNCAO = list(range(2020, ano_atual + 1))
ANOS_RRO = list(range(2020, ano_atual + 1))
ANOS_INDICADOR = list(range(2013, ano_atual + 1))
PERIODOS = [1, 2]

# ------------------------------
# Funções utilitárias
# ------------------------------
def proximo_ano_periodo(ano, periodo):
    return (ano, 2) if periodo == 1 else (ano + 1, 1)

def baixar_json(url):
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
        return data if data else None
    except Exception as e:
        print(f"❌ Erro ao baixar {url} - {e}")
        return None

def ultimo_arquivo_estado(pasta: Path, prefixo: str):
    arquivos_estado = list(pasta.glob(f"{prefixo}*.csv"))
    if not arquivos_estado:
        return None, None
    anos_periodos = []
    for f in arquivos_estado:
        numeros = ''.join(filter(str.isdigit, f.stem))
        if len(numeros) == 5:
            ano = int(numeros[:4])
            periodo = int(numeros[4])
            anos_periodos.append((ano, periodo))
    return max(anos_periodos) if anos_periodos else (None, None)


# ------------------------------
# Funções por endpoint
# ------------------------------
def baixar_subfuncao():
    output_dir = CSV_DIRS["siops_subfuncao"]
    output_dir.mkdir(parents=True, exist_ok=True)
    print("\n🚀 Baixando SIOPS Subfunção\n")

    for uf_sigla, uf_code in UFS.items():
        print(f"\n📌 UF {uf_sigla}")
        ano, periodo = ultimo_arquivo_estado(output_dir, uf_sigla)
        if ano is None:
            ano, periodo = ANOS_SUBFUNCAO[0], 1

        while ano <= ANOS_SUBFUNCAO[-1]:
            url = f"{BASE_URL}/despesas-por-subfuncao/{uf_code}/{ano}/{periodo}?page=0&size=10000"
            data = baixar_json(url)
            if data is None:
                print(f"⚠ Sem dados para {uf_sigla}/{ano}/b{periodo} - pulando")
                ano, periodo = proximo_ano_periodo(ano, periodo)
                continue
            df = pd.DataFrame(data)
            file_path = output_dir / f"{uf_sigla}{ano}{periodo}.csv"
            df.to_csv(file_path, index=False, encoding="utf-8-sig", sep=',')
            print(f"✔ Salvo: {file_path} ({len(df)} registros)")
            ano, periodo = proximo_ano_periodo(ano, periodo)
            time.sleep(0.2)

def baixar_rreo():
    output_dir = CSV_DIRS["siops_rro"]
    output_dir.mkdir(parents=True, exist_ok=True)
    print("\n🚀 Baixando RREO\n")

    # DF
    print("\n📌 DF")
    ano, periodo = ultimo_arquivo_estado(output_dir, "DF")
    if ano is None:
        ano, periodo = ANOS_RRO[0], 1

    while ano <= ANOS_RRO[-1]:
        url = f"{BASE_URL}/rreo/df/{ano}/{periodo}?page=0&size=10000"
        data = baixar_json(url)
        if data is None:
            print(f"⚠ Sem dados para DF/{ano}/b{periodo} - pulando")
            ano, periodo = proximo_ano_periodo(ano, periodo)
            continue
        df = pd.DataFrame(data)
        file_path = output_dir / f"DF{ano}{periodo}.csv"
        df.to_csv(file_path, index=False, encoding="utf-8-sig", sep=',')
        print(f"✔ DF salvo: {file_path} ({len(df)} registros)")
        ano, periodo = proximo_ano_periodo(ano, periodo)
        time.sleep(0.3)

    for uf_sigla, uf_code in UFS.items():
        if uf_sigla == "DF":
            continue
        print(f"\n📌 UF {uf_sigla}")
        ano, periodo = ultimo_arquivo_estado(output_dir, uf_sigla)
        if ano is None:
            ano, periodo = ANOS_RRO[0], 1

        while ano <= ANOS_RRO[-1]:
            url = f"{BASE_URL}/rreo/estadual/{uf_code}/{ano}/{periodo}?page=0&size=10000"
            data = baixar_json(url)
            if data is None:
                print(f"⚠ Sem dados para {uf_sigla}/{ano}/b{periodo} - pulando")
                ano, periodo = proximo_ano_periodo(ano, periodo)
                continue
            df = pd.DataFrame(data)
            file_path = output_dir / f"{uf_sigla}{ano}{periodo}.csv"
            df.to_csv(file_path, index=False, encoding="utf-8-sig", sep=',')
            print(f"✔ Salvo: {file_path} ({len(df)} registros)")
            ano, periodo = proximo_ano_periodo(ano, periodo)
            time.sleep(0.3)

def normalizar_indicador(df, uf, ano, periodo):
    df["uf"] = uf
    df["ano"] = ano
    df["periodo"] = periodo
    cols = ["uf", "ano", "periodo", "numero_indicador", "ds_indicador", "numerador", "denominador", "indicador_calculado"]
    return df[cols]

def baixar_indicador():
    output_dir = CSV_DIRS["siops_indicador"]
    output_dir.mkdir(parents=True, exist_ok=True)
    print("\n🚀 Baixando Indicadores\n")

    # DF
    print("\n📌 DF")
    ano, periodo = ultimo_arquivo_estado(output_dir, "DF")
    if ano is None:
        ano, periodo = ANOS_INDICADOR[0], 1

    while ano <= ANOS_INDICADOR[-1]:
        url = f"{BASE_URL}/indicador/df/{ano}/{periodo}?page=0&size=10000"
        data = baixar_json(url)
        if data is None:
            print(f"⚠ Sem dados para DF/{ano}/b{periodo} - pulando")
            ano, periodo = proximo_ano_periodo(ano, periodo)
            continue
        df = normalizar_indicador(pd.DataFrame(data), "DF", ano, periodo)
        file_path = output_dir / f"DF{ano}{periodo}.csv"
        df.to_csv(file_path, index=False, encoding="utf-8-sig", sep=',')
        print(f"✔ DF salvo: {file_path} ({len(df)} registros)")
        ano, periodo = proximo_ano_periodo(ano, periodo)
        time.sleep(0.2)

    for uf_sigla, uf_code in UFS.items():
        if uf_sigla == "DF":
            continue
        print(f"\n📌 UF {uf_sigla}")
        ano, periodo = ultimo_arquivo_estado(output_dir, uf_sigla)
        if ano is None:
            ano, periodo = ANOS_INDICADOR[0], 1

        while ano <= ANOS_INDICADOR[-1]:
            url = f"{BASE_URL}/indicador/estadual/{uf_code}/{ano}/{periodo}?page=0&size=10000"
            data = baixar_json(url)
            if data is None:
                print(f"⚠ Sem dados para {uf_sigla}/{ano}/b{periodo} - pulando")
                ano, periodo = proximo_ano_periodo(ano, periodo)
                continue
            df = normalizar_indicador(pd.DataFrame(data), uf_sigla, ano, periodo)
            file_path = output_dir / f"{uf_sigla}{ano}{periodo}.csv"
            df.to_csv(file_path, index=False, encoding="utf-8-sig", sep=',')
            print(f"✔ Salvo: {file_path} ({len(df)} registros)")
            ano, periodo = proximo_ano_periodo(ano, periodo)
            time.sleep(0.2)


# ------------------------------
# Execução principal
# ------------------------------
if __name__ == "__main__":
    baixar_subfuncao()
    baixar_rreo()
    baixar_indicador()
