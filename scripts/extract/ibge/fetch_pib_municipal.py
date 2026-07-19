"""IBGE - PIB Municipal (extract).

Fonte: API SIDRA, tabela 5938 (PIB por município e atividade econômica).
"""
from datetime import datetime

import requests

from scripts.common.paths import LANDING_DIR
from scripts.common.bucket_sync import carregar_manifesto
from scripts.common import exit_codes

JSON_DIR = LANDING_DIR / "json_ibge_pib"
PASTA_BUCKET = "ibge"
TABELA_SIDRA = 5938


def descobrir_nivel_territorial(tabela: int) -> str:
    """Retorna nível territorial (preferência: N6/município, fallback outro)."""
    url = f"https://servicodados.ibge.gov.br/api/v3/agregados/{tabela}/metadados"
    resposta = requests.get(url, timeout=30)
    resposta.raise_for_status()
    metadados = resposta.json()

    niveis = metadados.get("nivelTerritorial", {})
    todos_niveis = []
    for categoria, lista in niveis.items():
        todos_niveis.extend(lista)

    print(f"  Níveis territoriais disponíveis: {niveis}")

    # Preferência: município (N6) > qualquer outro nível mais fino disponível
    if "N6" in todos_niveis:
        return "N6"
    if todos_niveis:
        escolhido = todos_niveis[0]
        print(f"  [AVISO] N6 (município) não disponível nessa tabela -- usando '{escolhido}' em vez disso.")
        return escolhido
    raise ValueError(f"Tabela {tabela} não declara nenhum nível territorial nos metadados.")


def descobrir_variavel_e_classificacoes(tabela: int) -> tuple[int, dict[int, int]]:
    """Retorna (variável_principal, {classificação_id: categoria_total})."""
    url = f"https://servicodados.ibge.gov.br/api/v3/agregados/{tabela}/metadados"
    resposta = requests.get(url, timeout=30)
    resposta.raise_for_status()
    metadados = resposta.json()

    variaveis = metadados.get("variaveis", [])
    if not variaveis:
        raise ValueError(f"Tabela {tabela} não tem nenhuma variável declarada nos metadados.")
    variavel_principal = variaveis[0]["id"]
    print(f"  Variável principal escolhida: {variavel_principal} ({variaveis[0]['nome']}) -- de {len(variaveis)} disponíveis.")

    classificacoes = metadados.get("classificacoes", [])
    classificacoes_fixadas = {}
    for c in classificacoes:
        categorias = c.get("categorias", [])
        # categoria 0 é sempre "Total" por convenção do SIDRA
        tem_total = any(cat["id"] == 0 for cat in categorias)
        categoria_escolhida = 0 if tem_total else categorias[0]["id"]
        classificacoes_fixadas[c["id"]] = categoria_escolhida
        print(f"  Classificação {c['id']} ({c['nome']}): fixada em categoria {categoria_escolhida} (Total).")

    return variavel_principal, classificacoes_fixadas


def descobrir_anos_validos(tabela: int) -> list[int]:
    url = f"https://servicodados.ibge.gov.br/api/v3/agregados/{tabela}/periodos"
    resposta = requests.get(url, timeout=30)
    resposta.raise_for_status()
    periodos = resposta.json()
    return sorted(int(p["id"]) for p in periodos if p["id"].isdigit())


def url_ano(ano: int, nivel_territorial: str, variavel: int, classificacoes_fixadas: dict[int, int]) -> str:
    nivel_minusculo = nivel_territorial.lower()
    base = f"https://apisidra.ibge.gov.br/values/t/{TABELA_SIDRA}/{nivel_minusculo}/all/p/{ano}/v/{variavel}"
    for classificacao_id, categoria_id in classificacoes_fixadas.items():
        base += f"/c{classificacao_id}/{categoria_id}"
    return base + "?formato=json"


if __name__ == "__main__":
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    manifesto = carregar_manifesto(PASTA_BUCKET)

    try:
        nivel_territorial = descobrir_nivel_territorial(TABELA_SIDRA)
        print(f"Nível territorial escolhido: {nivel_territorial}")
    except (requests.RequestException, ValueError) as e:
        print(f"[ERRO] Não consegui descobrir o nível territorial da tabela {TABELA_SIDRA}: {e}")
        exit(exit_codes.ERRO)

    try:
        anos_validos = descobrir_anos_validos(TABELA_SIDRA)
        print(f"Anos válidos: {anos_validos[0]}-{anos_validos[-1]} ({len(anos_validos)} anos)")
    except requests.RequestException as e:
        print(f"[ERRO] Não consegui descobrir períodos da tabela {TABELA_SIDRA}: {e}")
        exit(exit_codes.ERRO)

    try:
        variavel_principal, classificacoes_fixadas = descobrir_variavel_e_classificacoes(TABELA_SIDRA)
    except (requests.RequestException, ValueError) as e:
        print(f"[ERRO] Não consegui descobrir variável/classificações da tabela {TABELA_SIDRA}: {e}")
        exit(exit_codes.ERRO)

    houve_novidade = False

    for ano in anos_validos:
        chave_manifesto = f"pib_{ano}.json"
        destino = JSON_DIR / chave_manifesto

        if destino.exists():
            continue  # já baixado localmente, não precisa buscar de novo

        try:
            resposta = requests.get(url_ano(ano, nivel_territorial, variavel_principal, classificacoes_fixadas), timeout=60)
        except requests.RequestException as e:
            print(f"[SKIP] {ano} -- erro de conexão: {e}")
            continue

        if resposta.status_code != 200:
            print(f"[AVISO] {ano} -- status {resposta.status_code}: {resposta.text[:150]}")
            continue

        dados = resposta.json()
        if not dados or len(dados) <= 1:
            print(f"[SKIP] {ano} -- resposta vazia.")
            continue

        tamanho_atual = len(resposta.content)
        if manifesto.get(chave_manifesto) == tamanho_atual:
            print(f"[SKIP] {ano} -- sem mudança desde a última execução.")
            continue

        destino.write_bytes(resposta.content)
        print(f"[OK] {ano}: {len(dados) - 1} linhas baixadas.")
        houve_novidade = True

    if not houve_novidade:
        print("[INFO] Nenhum ano novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)