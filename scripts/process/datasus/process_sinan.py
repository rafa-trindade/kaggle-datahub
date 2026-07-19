"""SINAN - process de todos os agravos (cada um em parquet próprio)."""
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes
from scripts.process.datasus.base_process_dbc import processar_fonte_ftp_incremental
from scripts.config.agravos_sinan import AGRAVOS_SINAN

PASTA_BUCKET = "sinan"

if __name__ == "__main__":
    algum_sucesso = False
    algum_erro = False

    for prefixo, nome_arquivo in AGRAVOS_SINAN:
        dbc_dir = LANDING_DIR / f"dbc_sinan_{nome_arquivo}"
        nome_arquivo_final = f"{nome_arquivo}.parquet"

        if not dbc_dir.exists():
            continue  

        print(f"=== {nome_arquivo} ({prefixo}) ===")
        codigo = processar_fonte_ftp_incremental(dbc_dir, PASTA_BUCKET, nome_arquivo_final)

        if codigo == exit_codes.SUCESSO:
            algum_sucesso = True
        elif codigo == exit_codes.ERRO:
            algum_erro = True
        

    if algum_erro and not algum_sucesso:
        exit(exit_codes.ERRO)
    elif not algum_sucesso:
        print("\n[INFO] Nenhum agravo teve novidade pra processar.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)