"""Processamento particionado do PA (Produção Ambulatorial do SIA/SUS).

Devido ao volume (~10 mil arquivos, dezenas de GB), o PA é particionado por
competência (ano-mês) em parquets individuais. O reprocessamento é incremental,
afetando apenas as competências com novidades, mais uma margem de segurança.
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
    """Lê .dbf do PA via dbfread em chunks de DataFrames (string).

    Usa dbfread direto e um FieldParser customizado para contornar headers 
    malformados e sujeira em campos numéricos (comuns em arquivos antigos) 
    que causariam erros de conversão no parser padrão.
    """
    from dbfread import FieldParser

    class _ParserTolerante(FieldParser):
        """Parser resiliente: em caso de falha na conversão, limpa o texto ou devolve o dado cru (string)."""
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

# Quantidade de meses recentes a reprocessar sempre (default 0 confia na detecção por tamanho).
# Reprocessar à toa custa ~1h/mês. Para forçar varredura no passado, use a env var 
# (ex: PA_COMPETENCIAS_MARGEM=120 para os últimos 10 anos).
COMPETENCIAS_MARGEM = int(os.environ.get("PA_COMPETENCIAS_MARGEM", "0"))


def _competencia_de(nome_dbc: str) -> str | None:
    """Extrai a competência AAAAMM de um nome no padrão PA (ex: PASP2401a.dbc -> 202401)."""
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
    """Retorna competências com arquivos novos, alterados ou dentro da margem de segurança."""
    afetadas: set[str] = set()

    # 1) novidade/alteração por arquivo-fonte
    for comp, arquivos in grupos.items():
        for arq in arquivos:
            chave = arq.upper()
            if chave not in manifesto or manifesto[chave] != tamanhos_atuais[arq]:
                afetadas.add(comp)
                break

    # 2) margem de segurança: N competências mais recentes presentes no disco
    competencias_ordenadas = sorted(grupos.keys()) 
    
    # Guard explícito, pois list[-0:] retorna a lista inteira (reprocessaria tudo)
    if COMPETENCIAS_MARGEM > 0:
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
    """Processa o PA em parquets isolados por competência.

    Se `filtro` for passado, executa Backfill explícito (ignora manifesto e margem).
    Caso contrário, opera em modo Incremental (novidades + margem de segurança).
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
        # MODO INCREMENTAL: novidades + margem de segurança
        afetadas = _competencias_afetadas(grupos, tamanhos_atuais, manifesto)

        novidade_real = any(
            arq.upper() not in manifesto or manifesto[arq.upper()] != tamanhos_atuais[arq]
            for arq in arquivos
        )
        if not novidade_real and manifesto:
            logger.info("[PA] Nenhuma competência nova ou alterada desde a última execução.")
            return exit_codes.SEM_NOVIDADE

        _txt_margem = (f"inclui margem de {COMPETENCIAS_MARGEM} competência(s) recentes"
                       if COMPETENCIAS_MARGEM > 0
                       else "sem margem -- só competências novas/alteradas por tamanho")
        logger.info(
            f"[PA] Modo incremental: {len(grupos)} competências no disco; "
            f"{len(afetadas)} serão (re)processadas ({_txt_margem})."
        )

    temp_dir = dbc_dir / "temp_pa"
    temp_dir.mkdir(exist_ok=True)
    DUCKDB_TEMP_DIR.mkdir(parents=True, exist_ok=True)

    s3 = get_s3_client()
    houve_erro = False

    # Checkpoint periódico do manifesto evita retrabalho em caso de interrupção (Ctrl+C).
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
