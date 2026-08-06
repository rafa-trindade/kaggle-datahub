"""Verificação de cobertura do PA (Produção Ambulatorial) do SIA/SUS.

Modos de uso:
  --ftp    : Lista competências disponíveis no FTP do DATASUS (1994-2007 e 2008+).
  --local  : Analisa os .dbc baixados em landing/dbc_sia_pa/ (arquivos, meses e tamanho).
  --bucket : Conta parquets publicados no MinIO por ano e aponta meses faltantes.
"""
import os
import re
import sys
import socket
from collections import defaultdict
from ftplib import error_perm

from scripts.common.paths import LANDING_DIR

DIRETORIO_FTP_ANTIGO = "/dissemin/publicos/SIASUS/199407_200712/Dados"
DIRETORIO_FTP_MODERNO = "/dissemin/publicos/SIASUS/200801_/Dados"
FTP_HOST = "ftp.datasus.gov.br"

# pasta/prefixo do PA no bucket (bate com base_process_pa.NOME_FONTE)
NOME_FONTE = "producao_ambulatorial"


def _fmt_tamanho(n_bytes: int) -> str:
    """Formata bytes em unidade legível (KiB/MiB/GiB)."""
    tam = float(n_bytes)
    for unidade in ("B", "KiB", "MiB", "GiB", "TiB"):
        if tam < 1024 or unidade == "TiB":
            return f"{tam:,.1f} {unidade}"
        tam /= 1024


def _competencia_dbc(nome: str):
    """Extrai (ano, mês) do nome do arquivo .dbc (ex: 'PASP2401a.dbc' -> 2024, 1)."""

    m = re.match(r"^PA[A-Z]{2}(\d{2})(\d{2})", nome, re.I)
    if not m or not nome.upper().endswith(".DBC"):
        return None
    aa, mm = int(m.group(1)), int(m.group(2))
    ano = 1900 + aa if aa >= 94 else 2000 + aa
    return ano, mm


def _competencia_parquet(nome: str):
    """De 'producao_ambulatorial_202401.parquet' extrai (2024, 1). None se não casar."""
    m = re.match(rf"^{NOME_FONTE}_(\d{{4}})(\d{{2}})\.parquet$", nome, re.I)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def listar_ftp():
    """Lista competências nos dois diretórios do FTP, sem baixar nada."""
    from scripts.extract.datasus.base_ftp import FTPPasvFix

    for label, diretorio in [("ANTIGO (1994-2007)", DIRETORIO_FTP_ANTIGO),
                             ("MODERNO (2008+)", DIRETORIO_FTP_MODERNO)]:
        print(f"\n=== {label}: {diretorio} ===")
        try:
            ip = socket.gethostbyname(FTP_HOST)
            with FTPPasvFix() as ftp:
                ftp.connect(ip, 21, timeout=30)
                ftp.login()
                ftp.set_pasv(True)
                ftp.cwd(diretorio)
                nomes = ftp.nlst()
        except (error_perm, OSError) as e:
            print(f"  [ERRO] não foi possível listar: {e}")
            continue

        comps = [_competencia_dbc(n) for n in nomes]
        comps = [c for c in comps if c]
        if not comps:
            print("  (nenhum arquivo PA encontrado)")
            continue
        anos = sorted({a for a, _ in comps})
        comps_ord = sorted(comps)
        print(f"  {len(nomes)} arquivos | anos: {anos[0]}..{anos[-1]}")
        print(f"  primeira competência: {comps_ord[0][0]}-{comps_ord[0][1]:02d}")
        print(f"  última competência:   {comps_ord[-1][0]}-{comps_ord[-1][1]:02d}")


def analisar_local():
    """Por ano: nº de arquivos-fonte, nº de competências e tamanho em disco."""
    pasta = LANDING_DIR / "dbc_sia_pa"
    if not pasta.exists():
        print(f"[ERRO] pasta não existe ainda: {pasta}")
        return

    arquivos_por_ano = defaultdict(int)
    bytes_por_ano = defaultdict(int)
    meses_por_ano = defaultdict(set)

    for nome in os.listdir(pasta):
        c = _competencia_dbc(nome)
        if not c:
            continue
        ano, mes = c
        arquivos_por_ano[ano] += 1
        meses_por_ano[ano].add(mes)
        try:
            bytes_por_ano[ano] += (pasta / nome).stat().st_size
        except OSError:
            pass

    if not arquivos_por_ano:
        print(f"[INFO] nenhum .dbc de PA em {pasta} ainda.")
        return

    anos = sorted(arquivos_por_ano)
    print(f"Arquivos PA em disco por ano (pasta: {pasta}):\n")
    print(f"  {'ano':>4} | {'arquivos':>8} | {'meses':>5} | {'tamanho':>12}")
    print(f"  {'-'*4}-+-{'-'*8}-+-{'-'*5}-+-{'-'*12}")
    total_arq = total_bytes = 0
    for ano in range(anos[0], anos[-1] + 1):
        n = arquivos_por_ano.get(ano, 0)
        nm = len(meses_por_ano.get(ano, ()))
        nb = bytes_por_ano.get(ano, 0)
        marca = "" if n else "   <-- SEM ARQUIVOS"
        print(f"  {ano:>4} | {n:>8} | {nm:>5} | {_fmt_tamanho(nb):>12}{marca}")
        total_arq += n
        total_bytes += nb

    print(f"  {'-'*4}-+-{'-'*8}-+-{'-'*5}-+-{'-'*12}")
    print(f"  {'TOT':>4} | {total_arq:>8} | {'':>5} | {_fmt_tamanho(total_bytes):>12}")
    print(f"\nTotal: {total_arq} arquivos, de {anos[0]} a {anos[-1]}, "
          f"{_fmt_tamanho(total_bytes)} em disco.")
    print("Dica: 'meses' é o nº de competências do ano; 'arquivos' pode ser maior "
          "(o DATASUS fatia meses grandes em partes a/b/c...).")


def analisar_bucket():
    """Conta os parquets de competência já publicados no MinIO, por ano."""
    from scripts.common.bucket_sync import get_s3_client
    from scripts.common import env

    s3 = get_s3_client()
    bucket = env.MINIO_BUCKET
    prefixo = f"{NOME_FONTE}/"

    comps_por_ano = defaultdict(set)
    bytes_por_ano = defaultdict(int)
    total_objetos = 0

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefixo):
        for obj in page.get("Contents", []):
            nome = os.path.basename(obj["Key"])
            c = _competencia_parquet(nome)
            if not c:
                continue
            ano, mes = c
            comps_por_ano[ano].add(mes)
            bytes_por_ano[ano] += obj["Size"]
            total_objetos += 1

    if not comps_por_ano:
        print(f"[INFO] nenhum parquet de PA publicado ainda em "
              f"s3://{bucket}/{prefixo} (processamento não iniciado?).")
        return

    anos = sorted(comps_por_ano)
    print(f"Parquets de PA publicados por ano (s3://{bucket}/{prefixo}):\n")
    print(f"  {'ano':>4} | {'competências':>12} | {'tamanho':>12} | faltantes")
    print(f"  {'-'*4}-+-{'-'*12}-+-{'-'*12}-+-{'-'*20}")
    total_comps = total_bytes = 0
    for ano in range(anos[0], anos[-1] + 1):
        meses = comps_por_ano.get(ano, set())
        nb = bytes_por_ano.get(ano, 0)
        # meses esperados: 1..12, exceto 1994 (jul+) -- mostra o que falta dentro do que existe
        esperados = set(range(1, 13))
        faltam = sorted(esperados - meses)
        falta_str = ""
        if meses and faltam:
            falta_str = "meses " + ",".join(f"{m:02d}" for m in faltam)
        print(f"  {ano:>4} | {len(meses):>12} | {_fmt_tamanho(nb):>12} | {falta_str}")
        total_comps += len(meses)
        total_bytes += nb

    print(f"  {'-'*4}-+-{'-'*12}-+-{'-'*12}-+-{'-'*20}")
    print(f"  {'TOT':>4} | {total_comps:>12} | {_fmt_tamanho(total_bytes):>12} |")
    print(f"\nTotal: {total_comps} competências publicadas, "
          f"{_fmt_tamanho(total_bytes)} em parquet.")
    print("Obs.: 'faltantes' lista meses de 01-12 ausentes DENTRO de anos que já "
          "têm algo publicado (1994 começa em jul; 2026 é parcial -- normal).")


def main():
    if "--ftp" in sys.argv:
        listar_ftp()
    elif "--local" in sys.argv:
        analisar_local()
    elif "--bucket" in sys.argv:
        analisar_bucket()
    else:
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())