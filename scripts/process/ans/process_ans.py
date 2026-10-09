"""ANS - base TabNet/TabWin (.dbc) -- process de todas as tabelas
configuradas em scripts/config/bases_ans.py. Cada tabela vira um parquet
próprio em ans/.

Os .dbc da ANS usam o mesmo formato dos .dbc do DATASUS (TabWin), então
reaproveita integralmente o base_process_dbc:
  - modo "incremental" -> mescla com o parquet publicado (coluna
    _ARQUIVO_ORIGEM identifica o arquivo-fonte de cada linha)
  - modo "retrato"     -> substitui o parquet publicado por completo
"""
from scripts.common import exit_codes
from scripts.common.paths import LANDING_DIR
from scripts.config.bases_ans import BASES_ANS
from scripts.process.datasus.base_process_dbc import (
    processar_fonte_ftp_incremental, processar_fonte_ftp_substituicao_completa,
)

PASTA_BUCKET = "ans"


def main() -> int:
    algum_sucesso = False
    algum_erro = False

    for base in BASES_ANS:
        dbc_dir = LANDING_DIR / f"dbc_ans_{base.nome_arquivo}"
        if not dbc_dir.exists():
            continue

        nome_final = f"{base.nome_arquivo}.parquet"
        print(f"\n=== ANS: {base.nome_arquivo} (modo {base.modo}) ===")

        if base.modo == "retrato":
            codigo = processar_fonte_ftp_substituicao_completa(
                dbc_dir, PASTA_BUCKET, nome_final, chave_manifesto_prefixo=base.prefixo,
            )
        else:
            codigo = processar_fonte_ftp_incremental(dbc_dir, PASTA_BUCKET, nome_final)

        if codigo == exit_codes.SUCESSO:
            algum_sucesso = True
        elif codigo == exit_codes.ERRO:
            algum_erro = True

    if algum_erro:
        return exit_codes.ERRO
    if not algum_sucesso:
        print("\n[INFO] Nenhuma tabela da ANS com novidade para processar.")
        return exit_codes.SEM_NOVIDADE
    return exit_codes.SUCESSO


if __name__ == "__main__":
    exit(main())
