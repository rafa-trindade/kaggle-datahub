"""SRAG (SIVEP-Gripe) -- extract.

1. Descobre os conjuntos "srag-*" no Portal de Dados Abertos da Saúde
   (API CKAN; se a API estiver fora, cai para leitura das páginas HTML).
2. Coleta os links diretos dos arquivos anuais INFLUDAA (prefere Parquet;
   usa CSV quando é o único formato -- caso de 2009-2018).
3. Confere o tamanho (HEAD) contra o manifesto srag/_manifest.json e baixa
   só os anos novos ou republicados para data/landing/srag/<era>/,
   com nome lógico estável (ex.: INFLUD25.parquet), independente da data
   embutida no nome publicado.
"""
import re

from scripts.common import exit_codes
from scripts.common.paths import LANDING_DIR
from scripts.common.bucket_sync import carregar_manifesto
from scripts.config.srag import PORTAL_SAUDE, TERMO_BUSCA, PASTA_BUCKET, era_do_ano
from scripts.extract.dados_abertos.base_http import criar_sessao, tamanho_remoto, baixar_http

LANDING_SRAG = LANDING_DIR / "srag"

# INFLUD25.parquet, INFLUD25-28-09-2026.parquet, INFLUD18.csv ...
PADRAO_ARQUIVO = re.compile(r"INFLUD(\d{2})(?:[-_][\d-]+)?\.(parquet|csv)$", re.IGNORECASE)
PADRAO_URL = re.compile(r"https?://[^\s\"'<>]+?INFLUD\d{2}(?:[-_][\d-]+)?\.(?:parquet|csv)", re.IGNORECASE)


# ----------------------------------------------------------------------
# Descoberta dos conjuntos e dos links
# ----------------------------------------------------------------------
def _conjuntos_via_api(sessao) -> list[str]:
    r = sessao.get(f"{PORTAL_SAUDE}/api/3/action/package_search",
                   params={"q": TERMO_BUSCA, "rows": 100}, timeout=60)
    r.raise_for_status()
    pacotes = r.json()["result"]["results"]
    return sorted({p["name"] for p in pacotes if p["name"].startswith("srag-")})


def _conjuntos_via_html(sessao) -> list[str]:
    r = sessao.get(f"{PORTAL_SAUDE}/dataset", params={"q": TERMO_BUSCA}, timeout=60)
    r.raise_for_status()
    return sorted(set(re.findall(r"/dataset/(srag-[a-z0-9-]+)", r.text)))


def _urls_via_api(sessao, conjunto: str) -> list[str]:
    r = sessao.get(f"{PORTAL_SAUDE}/api/3/action/package_show", params={"id": conjunto}, timeout=60)
    r.raise_for_status()
    return [res.get("url", "") for res in r.json()["result"].get("resources", [])]


def _urls_via_html(sessao, conjunto: str) -> list[str]:
    """Fallback: página do conjunto -> página de cada recurso -> link direto."""
    r = sessao.get(f"{PORTAL_SAUDE}/dataset/{conjunto}", timeout=60)
    r.raise_for_status()
    urls = set(PADRAO_URL.findall(r.text))
    recursos = sorted(set(re.findall(rf"/dataset/{re.escape(conjunto)}/resource/([0-9a-f-]{{36}})", r.text)))
    for rid in recursos:
        try:
            pr = sessao.get(f"{PORTAL_SAUDE}/dataset/{conjunto}/resource/{rid}", timeout=60)
            pr.raise_for_status()
            urls.update(PADRAO_URL.findall(pr.text))
        except Exception as e:
            print(f"  [AVISO] recurso {rid} ilegível: {type(e).__name__}: {e}")
    return sorted(urls)


def _data_no_nome(url: str) -> str:
    """'INFLUD25-28-09-2026.parquet' -> '20260928' (vazio se não houver data)."""
    m = re.search(r"INFLUD\d{2}[-_](\d{2})-(\d{2})-(\d{4})", url, re.IGNORECASE)
    return f"{m.group(3)}{m.group(2)}{m.group(1)}" if m else ""


def descobrir_arquivos(sessao) -> dict[int, str]:
    """Retorna {ano: url_direta} -- um arquivo por ano, Parquet preferido."""
    try:
        conjuntos = _conjuntos_via_api(sessao)
        via_api = True
    except Exception as e:
        print(f"[AVISO] API CKAN indisponível ({type(e).__name__}: {e}) -- usando páginas HTML.")
        conjuntos = _conjuntos_via_html(sessao)
        via_api = False

    if not conjuntos:
        raise RuntimeError("Nenhum conjunto 'srag-*' encontrado no portal.")
    print(f"Conjuntos SRAG encontrados: {', '.join(conjuntos)}")

    candidatos: dict[int, dict[str, str]] = {}   # ano -> {ext: url}
    for conjunto in conjuntos:
        try:
            urls = _urls_via_api(sessao, conjunto) if via_api else _urls_via_html(sessao, conjunto)
        except Exception as e:
            print(f"[AVISO] API falhou para {conjunto} ({e}) -- tentando HTML.")
            urls = _urls_via_html(sessao, conjunto)

        for url in urls:
            m = PADRAO_ARQUIVO.search(url.split("?")[0])
            if not m:
                continue
            ano = 2000 + int(m.group(1))
            ext = m.group(2).lower()
            # se houver mais de uma versão do mesmo ano/formato, fica a de
            # data de publicação mais recente (embutida no nome, DD-MM-AAAA)
            atual = candidatos.setdefault(ano, {}).get(ext)
            if atual is None or _data_no_nome(url) > _data_no_nome(atual):
                candidatos[ano][ext] = url

    escolhidos = {}
    for ano, por_ext in sorted(candidatos.items()):
        escolhidos[ano] = por_ext.get("parquet") or por_ext["csv"]
    return escolhidos


# ----------------------------------------------------------------------
def main() -> int:
    sessao = criar_sessao()
    try:
        arquivos = descobrir_arquivos(sessao)
    except Exception as e:
        print(f"[ERRO] Falha na descoberta dos arquivos SRAG: {type(e).__name__}: {e}")
        return exit_codes.ERRO

    print(f"{len(arquivos)} ano(s) disponível(is): {min(arquivos)}-{max(arquivos)}")
    manifesto = {k.upper(): v for k, v in carregar_manifesto(PASTA_BUCKET).items()}

    algum_erro = False
    alguma_novidade = False
    for ano, url in arquivos.items():
        era = era_do_ano(ano)
        if era is None:
            print(f"[AVISO] Ano {ano} fora das eras configuradas -- ignorado.")
            continue
        ext = url.split("?")[0].rsplit(".", 1)[-1].lower()
        nome_logico = f"INFLUD{ano % 100:02d}.{ext}"

        try:
            tamanho = tamanho_remoto(sessao, url)
        except Exception as e:
            print(f"[ERRO] HEAD falhou para {url}: {type(e).__name__}: {e}")
            algum_erro = True
            continue

        if tamanho is not None and manifesto.get(nome_logico.upper()) == tamanho:
            print(f"[SKIP-MANIFESTO] {ano} ({nome_logico}) sem mudança.")
            continue

        ok = baixar_http(sessao, url, LANDING_SRAG / era / nome_logico, tamanho_esperado=tamanho)
        algum_erro = algum_erro or not ok
        alguma_novidade = alguma_novidade or ok

    if algum_erro:
        return exit_codes.ERRO
    if not alguma_novidade:
        print("[INFO] Nenhuma novidade no SRAG desde a última execução.")
        return exit_codes.SEM_NOVIDADE
    return exit_codes.SUCESSO


if __name__ == "__main__":
    exit(main())
