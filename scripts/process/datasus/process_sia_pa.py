"""SIA/SUS - PA (Produção Ambulatorial, Jul/1994-atual) -- process.

Uso
---
Rotina (incremental -- novidades/revisões + margem de segurança):

    python -m scripts.process.datasus.process_sia_pa

Backfill do primeiro processamento, fatiado para ter controle e retomada
(processa exatamente a janela pedida, sem margem e ignorando o manifesto):

    python -m scripts.process.datasus.process_sia_pa --ano 2024
    python -m scripts.process.datasus.process_sia_pa --competencia 202401
    python -m scripts.process.datasus.process_sia_pa --de 202001 --ate 202412

Os filtros são mutuamente exclusivos. Sem nenhum filtro, roda incremental.
"""
import argparse
import re

from scripts.common.paths import LANDING_DIR
from scripts.process.datasus.base_process_pa import processar_pa_particionado

DBC_DIR = LANDING_DIR / "dbc_sia_pa"


def _parse_args():
    p = argparse.ArgumentParser(
        description="Processa o PA (Produção Ambulatorial) particionado por competência."
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("--ano", metavar="AAAA",
                   help="Processa todas as competências de um ano (ex.: --ano 2024).")
    g.add_argument("--competencia", metavar="AAAAMM",
                   help="Processa uma única competência (ex.: --competencia 202401).")
    p.add_argument("--de", metavar="AAAAMM",
                   help="Início do intervalo (usar junto com --ate).")
    p.add_argument("--ate", metavar="AAAAMM",
                   help="Fim do intervalo (usar junto com --de).")
    return p.parse_args()


def _validar_e_montar_filtro(args) -> dict | None:
    """Converte os argumentos num dict de filtro para o processador, validando
    formato. Retorna None se nenhum filtro foi passado (modo incremental)."""
    tem_intervalo = bool(args.de or args.ate)

    if args.ano:
        if not re.fullmatch(r"\d{4}", args.ano):
            raise SystemExit(f"--ano inválido: '{args.ano}' (esperado AAAA, ex.: 2024).")
        return {"tipo": "ano", "valor": args.ano}

    if args.competencia:
        if not re.fullmatch(r"\d{6}", args.competencia):
            raise SystemExit(f"--competencia inválida: '{args.competencia}' (esperado AAAAMM).")
        mes = int(args.competencia[4:6])
        if not (1 <= mes <= 12):
            raise SystemExit(f"--competencia com mês inválido: '{args.competencia}'.")
        return {"tipo": "competencia", "valor": args.competencia}

    if tem_intervalo:
        if not (args.de and args.ate):
            raise SystemExit("--de e --ate devem ser usados juntos.")
        for rot, val in (("--de", args.de), ("--ate", args.ate)):
            if not re.fullmatch(r"\d{6}", val):
                raise SystemExit(f"{rot} inválido: '{val}' (esperado AAAAMM).")
        if args.de > args.ate:
            raise SystemExit(f"Intervalo invertido: --de {args.de} > --ate {args.ate}.")
        return {"tipo": "intervalo", "de": args.de, "ate": args.ate}

    return None  # sem filtro -> incremental


if __name__ == "__main__":
    args = _parse_args()
    filtro = _validar_e_montar_filtro(args)
    exit(processar_pa_particionado(DBC_DIR, filtro=filtro))
