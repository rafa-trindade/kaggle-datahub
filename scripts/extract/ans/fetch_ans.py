"""ANS - base TabNet/TabWin (.dbc) -- extract de todas as tabelas
configuradas em scripts/config/bases_ans.py.

Para cada tabela: lista o índice HTTP da subpasta, confere o tamanho de
cada .dbc (HEAD) contra o manifesto ans/_manifest.json e baixa só o que
for novo ou revisado para data/landing/dbc_ans_<tabela>/.

Uso: python -m scripts.extract.ans.fetch_ans [--so planos,ressarcimento_ao_sus]
"""
import argparse
import re

from scripts.common import exit_codes
from scripts.common.paths import LANDING_DIR
from scripts.common.bucket_sync import carregar_manifesto
from scripts.config.bases_ans import BASES_ANS, URL_BASE_ANS, BaseANS
from scripts.extract.dados_abertos.base_http import (
    criar_sessao, listar_indice_http, tamanho_remoto, baixar_http, url_arquivo,
)

PASTA_BUCKET = "ans"


def dir_landing(base: BaseANS):
    return LANDING_DIR / f"dbc_ans_{base.nome_arquivo}"


def sincronizar_base(sessao, base: BaseANS, manifesto: dict[str, int]) -> tuple[bool, bool]:
    """Retorna (sucesso, houve_novidade) para uma tabela da ANS."""
    url_dir = f"{URL_BASE_ANS}/{base.subpasta}/"
    padrao = re.compile(rf"^{re.escape(base.prefixo)}[\d-]+\.dbc$", re.IGNORECASE)

    try:
        arquivos = listar_indice_http(sessao, url_dir, padrao)
    except Exception as e:
        print(f"[ERRO] Não consegui listar {url_dir}: {type(e).__name__}: {e}")
        return False, False

    if not arquivos:
        print(f"[AVISO] Nenhum arquivo {base.prefixo}*.dbc em {url_dir}.")
        return True, False

    if base.modo == "retrato":
        # só a competência mais recente interessa (nomes AAAA-MM ordenam cronologicamente)
        arquivos = [max(arquivos)]

    print(f"{len(arquivos)} arquivo(s) no servidor. Conferindo tamanhos contra o manifesto...")

    sucesso_geral = True
    houve_novidade = False
    ja_ok = 0
    for nome in arquivos:
        url = url_arquivo(url_dir, nome)
        try:
            tamanho = tamanho_remoto(sessao, url)
        except Exception as e:
            print(f"[ERRO] HEAD falhou para {nome}: {type(e).__name__}: {e}")
            sucesso_geral = False
            continue

        if tamanho is not None and manifesto.get(nome.upper()) == tamanho:
            ja_ok += 1
            continue

        ok = baixar_http(sessao, url, dir_landing(base) / nome, tamanho_esperado=tamanho)
        sucesso_geral = sucesso_geral and ok
        houve_novidade = houve_novidade or ok

    if ja_ok:
        print(f"[SKIP-MANIFESTO] {ja_ok} arquivo(s) já incorporado(s) (tamanho bate).")
    return sucesso_geral, houve_novidade


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract das tabelas da ANS (base TabNet .dbc).")
    parser.add_argument("--so", type=str, default=None,
                        help="nomes de tabela separados por vírgula (ver scripts/config/bases_ans.py)")
    args = parser.parse_args()

    bases = BASES_ANS
    if args.so:
        filtro = {s.strip() for s in args.so.split(",")}
        bases = [b for b in BASES_ANS if b.nome_arquivo in filtro]

    manifesto = {k.upper(): v for k, v in carregar_manifesto(PASTA_BUCKET).items()}
    sessao = criar_sessao()

    algum_erro = False
    alguma_novidade = False
    for base in bases:
        print(f"\n=== ANS: {base.nome_arquivo} ({base.subpasta}, modo {base.modo}) ===")
        sucesso, novidade = sincronizar_base(sessao, base, manifesto)
        algum_erro = algum_erro or not sucesso
        alguma_novidade = alguma_novidade or novidade

    # Novidade parcial ainda é processada (o process roda independente),
    # mas o erro é sinalizado para aparecer no resumo do run_all.
    if algum_erro:
        return exit_codes.ERRO
    if not alguma_novidade:
        print("\n[INFO] Nenhuma novidade na ANS desde a última execução.")
        return exit_codes.SEM_NOVIDADE
    return exit_codes.SUCESSO


if __name__ == "__main__":
    exit(main())
