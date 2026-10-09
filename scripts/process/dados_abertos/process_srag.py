"""SRAG (SIVEP-Gripe) -- process.

Para cada era (ver scripts/config/srag.py) com arquivos novos na landing:
  1. converte cada arquivo anual (Parquet ou CSV) para um parquet
     intermediário com todas as colunas como texto (dado bruto, sem
     reinterpretação de tipos) + coluna _ARQUIVO_ORIGEM (ex.: INFLUD25.parquet);
  2. baixa o parquet da era já publicado no bucket, descarta as linhas dos
     anos que chegaram de novo (o "banco vivo" republica anos recentes) e
     une com os novos (UNION ALL BY NAME -- colunas novas viram nulas nos
     anos antigos);
  3. publica e atualiza o manifesto srag/_manifest.json.

Mesma mecânica de mesclagem incremental do SIM/SIH, adaptada para CSV/Parquet.
"""
import os
import shutil
import time
from pathlib import Path

import duckdb

from scripts.common import exit_codes
from scripts.common.paths import BASE_DIR, LANDING_DIR
from scripts.config.srag import ERAS_SRAG, PASTA_BUCKET

LANDING_SRAG = LANDING_DIR / "srag"
DUCKDB_TEMP_DIR = Path(os.environ.get("DUCKDB_TEMP_DIR", str(BASE_DIR / "data" / ".duckdb_temp")))


def _conectar() -> duckdb.DuckDBPyConnection:
    DUCKDB_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=":memory:", config={
        "temp_directory": str(DUCKDB_TEMP_DIR),
        "memory_limit": "4GB",
    })
    # a ordem das linhas dentro de cada ano não importa; liberar isso reduz
    # bastante o uso de memória nos anos grandes (2020-2022, >1 mi de linhas
    # e ~190 colunas cada)
    con.execute("SET preserve_insertion_order=false;")
    return con


def _sql(caminho: Path) -> str:
    return str(caminho).replace("'", "''")


def detectar_encoding(caminho: Path) -> str:
    """UTF-8 se o arquivo inteiro decodifica como UTF-8; senão Latin-1.

    Precisa ser decidido ANTES da leitura: com ignore_errors=true o DuckDB
    não falha num encoding errado, só descarta as linhas silenciosamente."""
    import codecs
    decodificador = codecs.getincrementaldecoder("utf-8")()
    try:
        with open(caminho, "rb") as f:
            for bloco in iter(lambda: f.read(8 * 1024 * 1024), b""):
                decodificador.decode(bloco)
            decodificador.decode(b"", final=True)
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def _contar_linhas(caminho: Path) -> int:
    with open(caminho, "rb") as f:
        return sum(bloco.count(b"\n") for bloco in iter(lambda: f.read(8 * 1024 * 1024), b""))


def converter_arquivo(con, origem: Path, destino: Path) -> int:
    """Origem (parquet/csv) -> parquet com colunas VARCHAR + _ARQUIVO_ORIGEM. Retorna nº de linhas."""
    rotulo = origem.name.replace("'", "''")
    if origem.suffix.lower() == ".parquet":
        leitura = f"read_parquet('{_sql(origem)}')"
    else:
        # CSVs do SRAG: separador ';' na maioria dos anos (o sniffer do DuckDB
        # detecta); encoding varia entre anos.
        encoding = detectar_encoding(origem)
        print(f"   {origem.name}: encoding {encoding}")
        leitura = (f"read_csv('{_sql(origem)}', header=true, all_varchar=true, "
                   f"ignore_errors=true, sample_size=-1, encoding='{encoding}')")

    con.execute(f"""
        COPY (
            SELECT COLUMNS(*)::VARCHAR, '{rotulo}' AS _ARQUIVO_ORIGEM
            FROM {leitura}
        ) TO '{_sql(destino)}' (FORMAT PARQUET, ROW_GROUP_SIZE 250000);
    """)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_sql(destino)}')").fetchone()[0]

    if origem.suffix.lower() == ".csv":
        # aviso se o leitor descartou linhas (linhas físicas - cabeçalho);
        # campos com quebra de linha entre aspas inflam essa conta, por isso
        # é aviso e não erro
        esperadas = max(_contar_linhas(origem) - 1, 0)
        if esperadas and n < esperadas * 0.99:
            print(f"   [AVISO] {origem.name}: {n} registros lidos de ~{esperadas} linhas "
                  f"-- confira o arquivo (linhas malformadas foram descartadas).")
    return n


def processar_era(con, nome_era: str) -> int:
    from scripts.common.bucket_sync import obter_publicado

    dir_era = LANDING_SRAG / nome_era
    arquivos = sorted(p for p in dir_era.glob("INFLUD*") if p.suffix.lower() in (".parquet", ".csv"))
    if not arquivos:
        return exit_codes.SEM_NOVIDADE

    print(f"\n=== SRAG: {nome_era} -- {len(arquivos)} arquivo(s) novo(s)/revisado(s) ===")
    tamanhos = {p.name: p.stat().st_size for p in arquivos}

    nome_final = f"{nome_era}.parquet"
    s3_key = f"{PASTA_BUCKET}/{nome_final}"
    final = dir_era / nome_final

    # Retomada: se uma execução anterior já consolidou esta era e só o upload
    # falhou (ex.: túnel caiu), reaproveita o consolidado em vez de refazer.
    if _consolidado_reaproveitavel(con, final, arquivos):
        total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_sql(final)}')").fetchone()[0]
        print(f"[RETOMADA] {nome_final} já consolidado numa execução anterior "
              f"({total} registros) -- pulando direto para o upload.", flush=True)
        return _publicar(final, s3_key, tamanhos, dir_era)
    final.unlink(missing_ok=True)

    temp_dir = dir_era / "_temp"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir()

    # 1) conversão
    for i, p in enumerate(arquivos, 1):
        destino = temp_dir / f"{p.stem}.parquet"
        tamanho_mb = p.stat().st_size / 1024 / 1024
        print(f"   [{i}/{len(arquivos)}] convertendo {p.name} ({tamanho_mb:.0f} MB)...", flush=True)
        inicio = time.time()
        n = converter_arquivo(con, p, destino)
        print(f"   {p.name}: {n} registros ({time.time() - inicio:.0f}s)", flush=True)

    # 2) mesclagem com o publicado
    # minio: baixa uma cópia para temp_dir; local: lê o próprio publicado
    existente = obter_publicado(s3_key, temp_dir / "_existente.parquet")
    tem_existente = existente is not None
    if tem_existente:
        print(f"Parquet publicado encontrado em {s3_key} -- mesclando.")
    else:
        print(f"Nada publicado ainda em {s3_key} -- primeira publicação.")

    novos_glob = _sql(temp_dir / "INFLUD*.parquet")
    # remove do publicado qualquer versão anterior dos anos que chegaram
    # (compara pelo "INFLUDAA", ignorando extensão -- cobre troca CSV->Parquet)
    anos = ", ".join(f"'{p.stem.upper()}'" for p in arquivos)
    if tem_existente:
        query = f"""
            SELECT * FROM read_parquet('{_sql(existente)}')
            WHERE upper(split_part(_ARQUIVO_ORIGEM, '.', 1)) NOT IN ({anos})
            UNION ALL BY NAME
            SELECT * FROM read_parquet('{novos_glob}', union_by_name=true)
        """
    else:
        query = f"SELECT * FROM read_parquet('{novos_glob}', union_by_name=true)"

    print(f"   consolidando {nome_final}...", flush=True)
    inicio = time.time()
    # grava em .tmp e só renomeia no fim: um consolidado pela metade nunca
    # é confundido com um pronto na retomada
    parcial = final.with_name(final.name + ".tmp")
    parcial.unlink(missing_ok=True)
    con.execute(f"COPY ({query}) TO '{_sql(parcial)}' (FORMAT PARQUET, ROW_GROUP_SIZE 250000);")
    os.replace(parcial, final)
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"   consolidado em {time.time() - inicio:.0f}s", flush=True)
    total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{_sql(final)}')").fetchone()[0]
    print(f"✔ {total} registros em {nome_final}")

    # 3) publicação + manifesto
    return _publicar(final, s3_key, tamanhos, dir_era)


def _consolidado_reaproveitavel(con, final: Path, arquivos: list[Path]) -> bool:
    """True se `final` existe, é legível, é mais novo que todos os arquivos
    de entrada da landing e contém todos os anos deles."""
    if not final.exists():
        return False
    if any(p.stat().st_mtime > final.stat().st_mtime for p in arquivos):
        return False  # algum ano chegou depois do consolidado -> refaz
    try:
        origens = {r[0].rsplit(".", 1)[0].upper() for r in con.execute(
            f"SELECT DISTINCT _ARQUIVO_ORIGEM FROM read_parquet('{_sql(final)}')").fetchall()}
    except Exception:
        return False  # arquivo corrompido/incompleto -> refaz
    return all(p.stem.upper() in origens for p in arquivos)


def _publicar(final: Path, s3_key: str, tamanhos: dict[str, int], dir_era: Path) -> int:
    from scripts.common.bucket_sync import upload_and_cleanup, carregar_manifesto, salvar_manifesto

    if not upload_and_cleanup(final, s3_key):
        print("   O consolidado ficou na landing; rode de novo com o túnel no ar para "
              "publicar sem reprocessar.", flush=True)
        return exit_codes.ERRO

    manifesto = {k.upper(): v for k, v in carregar_manifesto(PASTA_BUCKET).items()}
    for nome, tam in tamanhos.items():
        stem = nome.rsplit(".", 1)[0].upper()
        manifesto = {k: v for k, v in manifesto.items() if k.rsplit(".", 1)[0] != stem}
        manifesto[nome.upper()] = tam
    salvar_manifesto(PASTA_BUCKET, manifesto)

    shutil.rmtree(dir_era, ignore_errors=True)
    return exit_codes.SUCESSO


def main() -> int:
    if not LANDING_SRAG.exists():
        print(f"[INFO] {LANDING_SRAG} não existe -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    # confere o MinIO antes da conversão (falha cedo se o túnel estiver fora)
    from scripts.common.bucket_sync import get_s3_client
    try:
        get_s3_client()
    except Exception as e:
        print(f"❌ MinIO inacessível antes de começar ({type(e).__name__}: {e}). Nada foi processado.")
        return exit_codes.ERRO

    algum_sucesso = False
    algum_erro = False
    con = _conectar()
    try:
        for nome_era, _, _ in ERAS_SRAG:
            try:
                codigo = processar_era(con, nome_era)
            except Exception as e:
                print(f"❌ Falha no SRAG {nome_era}: {type(e).__name__}: {e}")
                codigo = exit_codes.ERRO
            algum_sucesso = algum_sucesso or codigo == exit_codes.SUCESSO
            algum_erro = algum_erro or codigo == exit_codes.ERRO
    finally:
        con.close()

    if algum_erro:
        return exit_codes.ERRO
    if not algum_sucesso:
        print("[INFO] Nenhuma era do SRAG com novidade.")
        return exit_codes.SEM_NOVIDADE
    return exit_codes.SUCESSO


if __name__ == "__main__":
    exit(main())
