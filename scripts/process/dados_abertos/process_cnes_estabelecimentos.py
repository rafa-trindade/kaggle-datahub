"""
CNES - Estabelecimentos de Saúde -- process

Sem filtro de especialidade -- cadastro completo de todos os
estabelecimentos de saúde do Brasil.
"""
import requests

from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes
from scripts.common.bucket_sync import carregar_manifesto, salvar_manifesto, upload_and_cleanup
from scripts.process.dados_abertos.base_process_abertos import query_para_parquet

LANDING_FILE = LANDING_DIR / "csv_cnes" / "cnes_estabelecimentos_raw.csv"
PASTA_BUCKET = "cnes"
CHAVE_MANIFESTO = "estabelecimentos_de_saude.csv"
NOME_ARQUIVO_FINAL = "estabelecimentos_de_saude.parquet"
URL_ORIGEM = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/cnes_estabelecimentos_csv.zip"


def main():
    if not LANDING_FILE.exists():
        print(f"[INFO] {LANDING_FILE} não existe -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    caminho_parquet_temp = LANDING_DIR / "csv_cnes" / NOME_ARQUIVO_FINAL
    query = f"""
        SELECT *
        FROM read_csv(
            '{LANDING_FILE}',
            delim=';',
            header=true,
            encoding='ISO_8859_1',
            all_varchar=true,
            ignore_errors=true
        )
    """
    query_para_parquet(query, caminho_parquet_temp)

    s3_key = f"{PASTA_BUCKET}/{NOME_ARQUIVO_FINAL}"
    sucesso = upload_and_cleanup(caminho_parquet_temp, s3_key)
    if not sucesso:
        return exit_codes.ERRO

    LANDING_FILE.unlink(missing_ok=True)

    try:
        resposta = requests.head(URL_ORIGEM, timeout=30, allow_redirects=True)
        tamanho_atual = int(resposta.headers.get("Content-Length", 0))
        if tamanho_atual > 0:
            manifesto = carregar_manifesto(PASTA_BUCKET)
            manifesto[CHAVE_MANIFESTO] = tamanho_atual
            salvar_manifesto(PASTA_BUCKET, manifesto)
    except requests.RequestException as e:
        print(f"[AVISO] Publicado com sucesso, mas não consegui atualizar o manifesto: {e}")

    return exit_codes.SUCESSO


if __name__ == "__main__":
    exit(main())