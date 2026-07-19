"""Orquestração completa do pipeline.

Sequencial: manifesto compartilhado por pasta_bucket.
Cada módulo é idempotente, seguro interromper e rodar de novo.

Uso: python -m scripts.run_all [--so sim,sinasc] [--pular sinan]
"""
import argparse
import subprocess
import sys
import time

from scripts.config.fontes import FONTES
from scripts.common import exit_codes

NOME_STATUS = {
    exit_codes.SUCESSO: "✔ SUCESSO",
    exit_codes.SEM_NOVIDADE: "= SEM NOVIDADE",
    exit_codes.ERRO: "✖ ERRO",
}


def rodar_modulo(modulo: str) -> int:
    """Roda módulo via subprocess (python -m ...)."""
    resultado = subprocess.run([sys.executable, "-m", modulo])
    return resultado.returncode


def rodar_fonte(fonte) -> dict:
    print(f"\n{'=' * 70}")
    print(f"FONTE: {fonte.id} ({fonte.nome})")
    print('=' * 70)

    resultado_extract = exit_codes.SEM_NOVIDADE
    for modulo in fonte.extract_modules:
        print(f"\n[extract] {modulo}")
        codigo = rodar_modulo(modulo)
        print(f"[extract] {modulo} -> {NOME_STATUS.get(codigo, f'código {codigo}')}")
        if codigo == exit_codes.ERRO:
            resultado_extract = exit_codes.ERRO
        elif codigo == exit_codes.SUCESSO and resultado_extract != exit_codes.ERRO:
            resultado_extract = exit_codes.SUCESSO

    # Process roda independente do extract (cada módulo é idempotente)
    resultado_process = exit_codes.SEM_NOVIDADE
    for modulo in fonte.process_modules:
        print(f"\n[process] {modulo}")
        codigo = rodar_modulo(modulo)
        print(f"[process] {modulo} -> {NOME_STATUS.get(codigo, f'código {codigo}')}")
        if codigo == exit_codes.ERRO:
            resultado_process = exit_codes.ERRO
        elif codigo == exit_codes.SUCESSO and resultado_process != exit_codes.ERRO:
            resultado_process = exit_codes.SUCESSO

    return {
        "fonte_id": fonte.id,
        "extract": resultado_extract,
        "process": resultado_process,
    }


def main():
    parser = argparse.ArgumentParser(description="Roda extract+process de todas as fontes do pipeline.")
    parser.add_argument("--so", type=str, default=None,
                         help="Lista de pasta_bucket separadas por vírgula -- roda só essas (ex: sim,sinasc)")
    parser.add_argument("--pular", type=str, default=None,
                         help="Lista de pasta_bucket separadas por vírgula -- roda tudo menos essas")
    args = parser.parse_args()

    fontes_a_rodar = list(FONTES)

    if args.so:
        pastas_incluidas = {p.strip() for p in args.so.split(",")}
        fontes_a_rodar = [f for f in fontes_a_rodar if f.pasta_bucket in pastas_incluidas]

    if args.pular:
        pastas_excluidas = {p.strip() for p in args.pular.split(",")}
        fontes_a_rodar = [f for f in fontes_a_rodar if f.pasta_bucket not in pastas_excluidas]

    if not fontes_a_rodar:
        print("[AVISO] Nenhuma fonte corresponde aos filtros --so/--pular. Nada a fazer.")
        return

    print(f"Rodando {len(fontes_a_rodar)} fonte(s): {', '.join(f.id for f in fontes_a_rodar)}")

    inicio = time.time()
    resultados = []
    try:
        for fonte in fontes_a_rodar:
            resultados.append(rodar_fonte(fonte))
    except KeyboardInterrupt:
        print("\n\n[INTERROMPIDO] Ctrl+C recebido -- parando aqui. "
              "Seguro rodar de novo depois, cada fonte já pula o que está feito.")

    duracao = time.time() - inicio
    horas, resto = divmod(duracao, 3600)
    minutos, segundos = divmod(resto, 60)

    print(f"\n\n{'=' * 70}")
    print(f"RESUMO -- {len(resultados)} de {len(fontes_a_rodar)} fonte(s) processada(s) "
          f"em {int(horas)}h{int(minutos):02d}m{int(segundos):02d}s")
    print('=' * 70)
    for r in resultados:
        print(f"  {r['fonte_id']:30s} extract={NOME_STATUS.get(r['extract'])!s:20s} "
              f"process={NOME_STATUS.get(r['process'])!s:20s}")

    algum_erro = any(r["extract"] == exit_codes.ERRO or r["process"] == exit_codes.ERRO for r in resultados)
    if algum_erro:
        print("\n[AVISO] Pelo menos uma fonte teve erro -- revise o log acima antes de publicar.")
        sys.exit(1)


if __name__ == "__main__":
    main()