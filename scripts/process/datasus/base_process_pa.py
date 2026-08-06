"""Processamento PARTICIONADO do PA (Produção Ambulatorial do SIA/SUS).

Por que o PA é diferente das outras fontes
------------------------------------------
As demais fontes viram um único parquet consolidado (merge incremental que
baixa o parquet inteiro, funde e reescreve). O PA é grande demais para isso:
são ~10 mil arquivos .dbc e dezenas de GB de parquet final. Baixar/reescrever/
subir esse volume a cada execução é inviável, e um parquet único estoura o
limite de tamanho por arquivo do Kaggle.

Estratégia adotada
------------------
Particionamos por COMPETÊNCIA (ano-mês). Cada competência vira um parquet
próprio, publicado numa pasta dedicada:

    producao_ambulatorial/
        producao_ambulatorial_199407.parquet
        producao_ambulatorial_199408.parquet
        ...
        producao_ambulatorial_202604.parquet
        _manifest.json

Vantagens: cada arquivo fica pequeno (bem abaixo do limite do Kaggle); o
incremental reescreve apenas as competências afetadas, não o dataset inteiro;
a ordenação alfabética coincide com a cronológica (AAAAMM, ano de 4 dígitos).

Unidade de rastreio x unidade de reprocessamento
------------------------------------------------
O DATASUS fatia competências grandes em partes (PASP2401a.dbc, ...b.dbc). O
MANIFESTO rastreia cada arquivo-fonte .dbc individual (nome -> tamanho). Mas a
UNIDADE DE REPROCESSAMENTO é a competência: se qualquer parte de uma
competência é nova ou mudou de tamanho, o parquet daquela competência inteira
é reescrito (garante consistência, sem merge parcial dentro do mês).

Margem de segurança
-------------------
O DATASUS costuma revisar competências recentes silenciosamente. Além das
competências detectadas como novas/alteradas pelo manifesto, reprocessamos
também as N competências mais recentes presentes no disco (COMPETENCIAS_MARGEM),
por garantia.
"""
import os
import re
import gc
import logging
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from dbfread import DBF

from scripts.common import exit_codes
from scripts.common.bucket_sync import (
    get_s3_client,
    upload_and_cleanup,
    carregar_manifesto,
    salvar_manifesto,
)
from scripts.common import env
from scripts.process.datasus.base_process_dbc import (
    listar_dbc_deduplicados,
    _remover_seguro,
    _quarentena,
    DUCKDB_TEMP_DIR,
    datasus_dbc,
)

logger = logging.getLogger(__name__)


def _ler_dbf_pa(caminho_dbf: str, chunksize: int = 250_000):
    """Lê um .dbf do PA usando SOMENTE dbfread, em chunks de DataFrames (string).

    Por que dbfread direto (sem simpledbf): os .dbf do PA -- sobretudo os
    antigos -- têm uma variante de header que o simpledbf lê torto (nomes de
    coluna com bytes \\x00 e ZERO registros, sem lançar erro). O dbfread lê os
    mesmos arquivos corretamente. Como isso vale para o PA inteiro, usamos
    dbfread direto aqui, eliminando a incerteza.

    Parser TOLERANTE: os dados antigos do PA (ex.: 2000) têm SUJEIRA em campos
    numéricos -- valores como b'"14.35' (com aspa) ou b'\\xa0 6.50' (espaço não
    quebrável). O parser padrão do dbfread tenta converter para float e explode
    (ValueError), fazendo a UF inteira ser (erroneamente) quarentenada. Como no
    nosso pipeline tudo vira string mesmo, usamos um FieldParser que, se a
    conversão numérica/data falhar, devolve o TEXTO CRU do campo em vez de
    lançar erro. Nada de dado bom é perdido e nada é quarentenado à toa.

    Normalizações (para casar com o resto do datalake):
      - nomes de coluna: remove bytes \\x00 e espaços das bordas;
      - valores: tudo string; vazios/None/espaços viram NaN.
    """
    from dbfread import FieldParser

    class _ParserTolerante(FieldParser):
        """Parser resiliente à sujeira dos dados antigos do PA.

        Campos numéricos às vezes trazem lixo (aspas, \\xa0, espaços). O parser
        padrão do dbfread lança ValueError e derruba a leitura da UF inteira.
        Aqui: se o parse falhar, limpamos os caracteres-lixo conhecidos e
        tentamos de novo; se ainda assim falhar, devolvemos o texto cru (nunca
        levanta exceção). Como tudo vira string no pipeline, nada se perde.
        """
        def parse(self, field, data):
            try:
                return super().parse(field, data)
            except (ValueError, TypeError):
                # remove lixo comum e tenta reparsear o número
                limpo = (data.replace(b'"', b'').replace(b'\xa0', b'')
                             .replace(b"'", b'').strip())
                if limpo:
                    try:
                        return super().parse(field, limpo)
                    except (ValueError, TypeError):
                        pass
                # último recurso: texto cru decodificado (sem quebrar)
                try:
                    txt = data.decode(self.dbf.encoding, errors="replace").strip()
                    return txt or None
                except Exception:
                    return None

    def _para_string(df: pd.DataFrame) -> pd.DataFrame:
        out = df.astype(str)
        for col in out.columns:
            vazio = out[col].str.strip().isin(["", "None", "nan", "NaT"])
            out[col] = out[col].mask(vazio, np.nan)
        return out

    def _limpar_cols(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=lambda c: str(c).replace("\x00", "").strip())

    tabela = DBF(caminho_dbf, encoding="latin1", ignore_missing_memofile=True,
                 char_decode_errors="replace", parserclass=_ParserTolerante)
    buffer = []
    for registro in tabela:
        buffer.append(registro)
        if len(buffer) >= chunksize:
            yield _limpar_cols(_para_string(pd.DataFrame(buffer)))
            buffer.clear()
            gc.collect()
    if buffer:
        yield _limpar_cols(_para_string(pd.DataFrame(buffer)))

# Nome descritivo (padrão do projeto) usado tanto para a pasta quanto para o
# prefixo dos arquivos de competência.
NOME_FONTE = "producao_ambulatorial"

# Quantas competências recentes reprocessar sempre, por garantia (revisões
# silenciosas do DATASUS nos meses mais novos).
COMPETENCIAS_MARGEM = 6


def _competencia_de(nome_dbc: str) -> str | None:
    """De 'PASP2401a.dbc' extrai a competência 'AAAAMM' = '202401'.

    Formato dos arquivos PA: PA{UF}{AA}{MM}[parte].dbc, onde AA são os 2
    últimos dígitos do ano (94-99 -> 1994-1999; 00-93 -> 2000-2093) e MM o mês.
    Retorna None se o nome não casar com o padrão do PA.
    """
    m = re.match(r"^PA[A-Z]{2}(\d{2})(\d{2})", nome_dbc.upper())
    if not m:
        return None
    aa, mm = int(m.group(1)), int(m.group(2))
    ano = 1900 + aa if aa >= 94 else 2000 + aa
    return f"{ano:04d}{mm:02d}"


def _agrupar_por_competencia(arquivos: list[str]) -> dict[str, list[str]]:
    """Agrupa a lista de .dbc por competência AAAAMM. Ignora nomes fora do padrão."""
    grupos: dict[str, list[str]] = {}
    for arq in arquivos:
        comp = _competencia_de(arq)
        if comp is None:
            logger.warning(f"[PA] Ignorando arquivo fora do padrão PA: {arq}")
            continue
        grupos.setdefault(comp, []).append(arq)
    return grupos


def _competencias_afetadas(
    grupos: dict[str, list[str]],
    tamanhos_atuais: dict[str, int],
    manifesto: dict[str, int],
) -> set[str]:
    """Decide quais competências reprocessar.

    Uma competência é reprocessada se:
      - alguma de suas partes .dbc é nova (não está no manifesto), OU
      - alguma parte mudou de tamanho em relação ao manifesto, OU
      - está entre as COMPETENCIAS_MARGEM competências mais recentes (margem
        de segurança contra revisões silenciosas).
    """
    afetadas: set[str] = set()

    # 1) novidade/alteração por arquivo-fonte
    for comp, arquivos in grupos.items():
        for arq in arquivos:
            chave = arq.upper()
            if chave not in manifesto or manifesto[chave] != tamanhos_atuais[arq]:
                afetadas.add(comp)
                break

    # 2) margem de segurança: N competências mais recentes presentes no disco
    competencias_ordenadas = sorted(grupos.keys())  # AAAAMM ordena cronologicamente
    margem = set(competencias_ordenadas[-COMPETENCIAS_MARGEM:])
    afetadas |= margem

    return afetadas


def _converter_competencia(
    dbc_dir: Path, competencia: str, arquivos: list[str], temp_dir: Path
) -> Path | None:
    """Converte todos os .dbc de uma competência num único parquet.

    Retorna o caminho do parquet gerado, ou None se nenhum arquivo pôde ser
    convertido. Reusa o leitor unificado (simpledbf -> dbfread) do módulo base.
    """
    parquet_saida = temp_dir / f"{NOME_FONTE}_{competencia}.parquet"
    writer = None
    schema_ref = None            # schema de referência (1ª parte escrita)
    colunas_ref = None           # ordem de colunas de referência
    convertidos = 0
    linhas_total = 0

    for arquivo in arquivos:
        caminho_dbc = str(dbc_dir / arquivo)
        caminho_dbf = caminho_dbc.replace(".DBC", ".DBF").replace(".dbc", ".dbf")
        try:
            if os.path.exists(caminho_dbf):
                os.remove(caminho_dbf)
            datasus_dbc.decompress(caminho_dbc, caminho_dbf)

            for df_chunk in _ler_dbf_pa(caminho_dbf, chunksize=250_000):
                df_chunk["_ARQUIVO_ORIGEM"] = arquivo

                # Alinha o schema entre partes/UFs da mesma competência. Arquivos
                # de UFs diferentes podem trazer colunas em ordem distinta (ou uma
                # a mais/menos). Sem alinhar, o ParquetWriter -- preso ao schema da
                # 1ª parte -- rejeitaria as demais e a competência sairia truncada.
                # Fixamos a ordem de colunas da 1ª parte e reindexamos todas as
                # seguintes a ela (colunas ausentes viram NaN; colunas novas são
                # anexadas ao final do schema de referência).
                if colunas_ref is None:
                    colunas_ref = list(df_chunk.columns)
                elif list(df_chunk.columns) != colunas_ref:
                    for c in df_chunk.columns:
                        if c not in colunas_ref:
                            logger.warning(
                                f"[PA {competencia}] coluna nova '{c}' em {arquivo} "
                                f"-- anexada ao schema."
                            )
                            colunas_ref.append(c)
                    df_chunk = df_chunk.reindex(columns=colunas_ref)

                # tudo é string no pipeline do PA; força dtype uniforme para o
                # schema do parquet ser estável entre as partes
                table = pa.Table.from_pandas(
                    df_chunk.astype("string"), preserve_index=False
                )
                if writer is None:
                    schema_ref = table.schema
                    writer = pq.ParquetWriter(str(parquet_saida), schema_ref)
                elif table.schema != schema_ref:
                    # reordena/completa para bater exatamente com o schema do writer
                    table = table.select([n for n in schema_ref.names])

                writer.write_table(table)
                linhas_total += table.num_rows
                del df_chunk, table
                gc.collect()

            convertidos += 1
            _remover_seguro(caminho_dbf)

        except Exception as e:
            logger.error(f"❌ [PA {competencia}] Falha ao converter {arquivo}: {type(e).__name__}: {e}")
            _remover_seguro(caminho_dbf)
            if os.path.exists(caminho_dbc):
                _quarentena(dbc_dir, caminho_dbc, arquivo)
            continue

    if writer is not None:
        writer.close()

    if convertidos == 0:
        logger.error(f"❌ [PA {competencia}] Nenhuma parte convertida.")
        _remover_seguro(str(parquet_saida))
        return None

    logger.info(f"[PA {competencia}] {convertidos} parte(s), {linhas_total:,} linha(s) no parquet.")

    if linhas_total == 0:
        logger.warning(
            f"⚠ [PA {competencia}] parquet gerado com 0 linhas -- verifique os .dbc de origem."
        )

    return parquet_saida


def processar_pa_particionado(dbc_dir: Path) -> int:
    """Processa o PA em parquets por competência e publica na pasta dedicada.

    Retorna exit code (SEM_NOVIDADE / ERRO / SUCESSO).
    """
def _competencias_no_filtro(
    competencias: list[str], filtro: dict | None
) -> list[str]:
    """Aplica o filtro de janela (--ano / --competencia / --de/--ate) à lista
    de competências AAAAMM presentes no disco.

    filtro é None (sem filtro) ou um dict com uma destas formas:
      {"tipo": "ano", "valor": "2024"}
      {"tipo": "competencia", "valor": "202401"}
      {"tipo": "intervalo", "de": "202001", "ate": "202412"}

    Retorna a sublista de `competencias` que casa com o filtro (ordenada).
    """
    if filtro is None:
        return sorted(competencias)

    tipo = filtro["tipo"]
    if tipo == "ano":
        ano = filtro["valor"]
        selec = [c for c in competencias if c[:4] == ano]
    elif tipo == "competencia":
        alvo = filtro["valor"]
        selec = [c for c in competencias if c == alvo]
    elif tipo == "intervalo":
        de, ate = filtro["de"], filtro["ate"]
        selec = [c for c in competencias if de <= c <= ate]
    else:
        selec = []
    return sorted(selec)


def processar_pa_particionado(dbc_dir: Path, filtro: dict | None = None) -> int:
    """Processa o PA em parquets por competência e publica na pasta dedicada.

    filtro (opcional) restringe QUAIS competências processar:
      - None (padrão): modo INCREMENTAL. Processa o que é novo/alterado segundo
        o manifesto + a margem de segurança de competências recentes. É o modo
        das atualizações de rotina.
      - dict (--ano/--competencia/--de-ate): modo BACKFILL EXPLÍCITO. Processa
        exatamente a janela pedida, IGNORANDO o manifesto e SEM margem (você
        está pedindo aquele período na marra; reprocessa mesmo se já existir).

    Retorna exit code (SEM_NOVIDADE / ERRO / SUCESSO).
    """
    if not dbc_dir.exists():
        logger.info(f"{dbc_dir} não existe -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    arquivos = listar_dbc_deduplicados(dbc_dir)
    if not arquivos:
        logger.info("[PA] Nenhum .dbc presente -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    tamanhos_atuais = {a: (dbc_dir / a).stat().st_size for a in arquivos}
    grupos = _agrupar_por_competencia(arquivos)
    if not grupos:
        logger.info("[PA] Nenhum arquivo no padrão PA encontrado -- nada a processar.")
        return exit_codes.SEM_NOVIDADE

    # manifesto próprio do PA (fica em producao_ambulatorial/_manifest.json)
    manifesto = carregar_manifesto(NOME_FONTE)
    manifesto = {k.upper(): v for k, v in manifesto.items()}

    if filtro is not None:
        # ---- MODO BACKFILL EXPLÍCITO: janela pedida, sem margem, ignora manifesto
        afetadas = set(_competencias_no_filtro(list(grupos.keys()), filtro))
        if not afetadas:
            logger.warning(
                f"[PA] Nenhuma competência no disco casa com o filtro {filtro}. "
                f"(Competências disponíveis: {min(grupos)}..{max(grupos)})"
            )
            return exit_codes.SEM_NOVIDADE
        logger.info(
            f"[PA] Modo backfill explícito ({filtro}): "
            f"{len(afetadas)} competência(s) serão (re)processadas, "
            f"sem margem e ignorando o manifesto."
        )
    else:
        # ---- MODO INCREMENTAL: novidades + margem de segurança
        afetadas = _competencias_afetadas(grupos, tamanhos_atuais, manifesto)

        novidade_real = any(
            arq.upper() not in manifesto or manifesto[arq.upper()] != tamanhos_atuais[arq]
            for arq in arquivos
        )
        if not novidade_real and manifesto:
            logger.info("[PA] Nenhuma competência nova ou alterada desde a última execução.")
            return exit_codes.SEM_NOVIDADE

        logger.info(
            f"[PA] Modo incremental: {len(grupos)} competências no disco; "
            f"{len(afetadas)} serão (re)processadas "
            f"(inclui margem de {COMPETENCIAS_MARGEM} competências recentes)."
        )

    temp_dir = dbc_dir / "temp_pa"
    temp_dir.mkdir(exist_ok=True)
    DUCKDB_TEMP_DIR.mkdir(parents=True, exist_ok=True)

    s3 = get_s3_client()
    houve_erro = False

    # Checkpoint: a cada quantas competências o manifesto é salvo durante o loop.
    # Sem isso, uma interrupção (Ctrl+C) antes do fim faria a próxima execução
    # reprocessar tudo de novo -- os parquets já subiram, mas o manifesto não
    # teria registrado. Não há perda de dado; o checkpoint evita RETRABALHO.
    CHECKPOINT_A_CADA = 10
    desde_ultimo_checkpoint = 0

    def _persistir_manifesto():
        try:
            salvar_manifesto(NOME_FONTE, manifesto)
        except Exception as e:
            logger.error(f"[PA] Falha ao salvar manifesto (checkpoint): {e}")

    try:
        for i, competencia in enumerate(sorted(afetadas), 1):
            arquivos_comp = grupos[competencia]
            logger.info(
                f"[PA] ({i}/{len(afetadas)}) Competência {competencia}: "
                f"{len(arquivos_comp)} arquivo(s)-fonte."
            )

            parquet_local = _converter_competencia(dbc_dir, competencia, arquivos_comp, temp_dir)
            if parquet_local is None:
                houve_erro = True
                continue

            s3_key = f"{NOME_FONTE}/{NOME_FONTE}_{competencia}.parquet"
            if not upload_and_cleanup(parquet_local, s3_key):
                logger.error(f"❌ [PA {competencia}] Falha no upload de {s3_key}.")
                houve_erro = True
                continue

            # sucesso nesta competência: registra suas partes no manifesto
            for arq in arquivos_comp:
                manifesto[arq.upper()] = tamanhos_atuais[arq]

            # checkpoint periódico do manifesto
            desde_ultimo_checkpoint += 1
            if desde_ultimo_checkpoint >= CHECKPOINT_A_CADA:
                _persistir_manifesto()
                desde_ultimo_checkpoint = 0

    except KeyboardInterrupt:
        # Interrupção manual: persiste o que já subiu antes de sair, para a
        # próxima execução retomar de onde parou (só reprocessa a margem).
        logger.warning("[PA] Interrompido pelo usuário -- salvando progresso no manifesto...")
        _persistir_manifesto()
        logger.warning("[PA] Progresso salvo. Rode novamente para retomar de onde parou.")
        return exit_codes.ERRO

    # salva o manifesto final (competências que subiram OK desde o último checkpoint)
    _persistir_manifesto()

    # limpeza do temp
    try:
        for f in temp_dir.iterdir():
            _remover_seguro(str(f))
        temp_dir.rmdir()
    except OSError:
        pass

    if houve_erro:
        logger.warning("[PA] Concluído com falhas em algumas competências (ver logs acima).")
        return exit_codes.ERRO

    logger.info("[PA] Processamento particionado concluído com sucesso.")
    return exit_codes.SUCESSO
