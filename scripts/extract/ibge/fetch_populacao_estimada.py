"""IBGE - População Estimada por Município (extract).

Fonte: API SIDRA, tabela 6579 (2001-atual). Um ano por vez.
"""
import json
from datetime import datetime

import requests

from scripts.common.paths import LANDING_DIR
from scripts.common.bucket_sync import carregar_manifesto
from scripts.common import exit_codes

JSON_DIR = LANDING_DIR / "json_ibge_populacao"
PASTA_BUCKET = "ibge"
ANO_INICIAL = 2001
TABELA_SIDRA = 6579
VARIAVEL_SIDRA = 9324


def url_ano(ano: int) -> str:
    return f"https://apisidra.ibge.gov.br/values/t/{TABELA_SIDRA}/n6/all/p/{ano}/v/{VARIAVEL_SIDRA}?formato=json"


if __name__ == "__main__":
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    manifesto = carregar_manifesto(PASTA_BUCKET)

    ano_atual = datetime.now().year
    houve_novidade = False
    sucesso_geral = True

    for ano in range(ANO_INICIAL, ano_atual + 1):
        chave_manifesto = f"populacao_{ano}.json"
        destino = JSON_DIR / chave_manifesto

        try:
            resposta = requests.get(url_ano(ano), timeout=60)
        except requests.RequestException as e:
            print(f"[SKIP] {ano} -- erro de conexão: {e}")
            continue

        if resposta.status_code != 200:
            print(f"[SKIP] {ano} -- ainda não publicado (status {resposta.status_code}).")
            continue

        dados = resposta.json()
        if not dados or len(dados) <= 1:
            print(f"[SKIP] {ano} -- resposta vazia (provavelmente ano ainda não publicado).")
            continue

        tamanho_atual = len(resposta.content)
        if manifesto.get(chave_manifesto) == tamanho_atual:
            print(f"[SKIP] {ano} -- sem mudança desde a última execução.")
            continue

        destino.write_bytes(resposta.content)  
        
        print(f"[OK] {ano}: {len(dados) - 1} municípios baixados.")
        houve_novidade = True

    if not houve_novidade:
        print("[INFO] Nenhum ano novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)