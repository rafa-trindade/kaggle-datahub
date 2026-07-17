"""
PNS/IBGE 2013 -- process (bruto)

Microdados de posição fixa completos, publicados COMO ESTÃO (sem
decodificar nenhuma variável) -- a PNS tem mais de 1000 variáveis, e
mapear posição por posição seria um projeto à parte, sem valor
suficiente pra esse hub. Quem for usar precisa do dicionário oficial
de posições do IBGE pra decodificar.

Não vira Parquet -- não há estrutura tabular pra preservar sem
decodificar as colunas primeiro, então o .txt original é o formato que
faz mais sentido aqui (é literalmente o que qualquer ferramenta oficial
de leitura, como sintaxes SPSS/SAS do IBGE, espera receber).
"""
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes
from scripts.common.bucket_sync import carregar_manifesto, salvar_manifesto, upload_and_cleanup

ARQUIVO_ENTRADA = LANDING_DIR / "ibge" / "PNS_2013.txt"
PASTA_BUCKET = "ibge"
CHAVE_MANIFESTO = "microdados_pns_2013.txt"
NOME_ARQUIVO_FINAL = "microdados_pns_2013.txt"


def main():
    if not ARQUIVO_ENTRADA.exists():
        print(f"[ERRO] {ARQUIVO_ENTRADA} não encontrado -- coloque o microdado bruto "
              f"(baixado manualmente do IBGE) nesse caminho antes de rodar.")
        return exit_codes.ERRO

    tamanho_atual = ARQUIVO_ENTRADA.stat().st_size
    manifesto = carregar_manifesto(PASTA_BUCKET)
    if manifesto.get(CHAVE_MANIFESTO) == tamanho_atual:
        print("[SKIP] Sem mudança desde a última execução.")
        return exit_codes.SEM_NOVIDADE

    s3_key = f"{PASTA_BUCKET}/{NOME_ARQUIVO_FINAL}"
    # upload_and_cleanup apagaria o .txt de origem -- aqui não queremos isso,
    # já que é um arquivo manual persistente, não um temporário de landing.
    sucesso = upload_and_cleanup(ARQUIVO_ENTRADA, s3_key, apagar_local=False)
    if not sucesso:
        return exit_codes.ERRO

    manifesto[CHAVE_MANIFESTO] = tamanho_atual
    salvar_manifesto(PASTA_BUCKET, manifesto)

    return exit_codes.SUCESSO


if __name__ == "__main__":
    exit(main())