"""Gera metadados.csv: manifesto de arquivos publicados com metadados.
"""
import csv
import datetime
import pyarrow.parquet as pq
import pyarrow.fs as pafs

from scripts.common.paths import BASE_DIR
from scripts.common import env
from scripts.common.bucket_sync import get_s3_client
from scripts.config.fontes import FONTES

NOME_ARQUIVO_SAIDA = "datahub-metadados.csv"
CAMINHO_LOCAL_PERSISTENTE = BASE_DIR / "docs" / NOME_ARQUIVO_SAIDA

# Metadados dedicado do PA
PREFIXO_PA = "producao_ambulatorial/"
NOME_METADADOS_PA = "_datahub-pa-metadados.csv"
CAMINHO_LOCAL_PA = BASE_DIR / "docs" / NOME_METADADOS_PA

COLUNAS = ["arquivo", "diretorio", "nome_fonte", "descricao", "tamanho_bytes", "num_registros", "ultima_atualizacao"]

FONTE_POR_ID = {f.id: f for f in FONTES}

MAPEAMENTO_ARQUIVO_ID = {
    # CNES
    "estabelecimentos_de_saude": "cnes_estabelecimentos",
    "habilitacoes": "cnes_habilitacoes",
    "leitos": "cnes_leitos",
    "profissionais": "cnes_profissionais",
    "equipamentos": "cnes_equipamentos",
    
    # SIM 
    "causas_externas_cid10": "sim_causas_externas_cid10",
    "causas_externas_cid9": "sim_causas_externas_cid9",
    "fetais_cid10": "sim_dofet_cid10",
    "fetais_cid9": "sim_dofet_cid9",
    "infantis_cid10": "sim_doinf_cid10",
    "infantis_cid9": "sim_doinf_cid9",
    "maternos_cid10": "sim_domat",
    "residentes_exterior": "sim_dorext",
    "declaracoes_de_obito_cid10": "sim_do_cid10",
    "declaracoes_de_obito_cid9": "sim_do_cid9",
    
    # SINASC
    "nascido_vivo_exterior": "sinasc_dnex",
    "nascido_vivo": "sinasc",
    
    # SIH
    "aih_reduzida": "sih_rd",
    "aih_rejeitada": "sih_rj",
    "servicos_profissionais": "sih_sp",

    # SIA 
    "producao_ambulatorial": "sia_pa",
    "apac_medicamentos": "sia_am",
    "apac_quimioterapia": "sia_aq",
    "apac_radioterapia": "sia_ar",
    "apac_nefrologia": "sia_an",
    "apac_tratamento_dialitico": "sia_atd",
    "apac_laudos_diversos": "sia_ad",
    "psicossocial": "sia_ps",
    "atencao_domiciliar": "sia_sad",
    "apac_confeccao_fistula": "sia_acf",
    "apac_pos_cirurgia_bariatrica": "sia_abo",
    "apac_cirurgia_bariatrica": "sia_ab",

    # CIHA
    "comunicacao_internacao_hospitalar_ambulatorial": "ciha",
    
    # IBGE e GEO
    "macroregiao_de_saude": "macroregiao",
    "populacao_estimada": "ibge_populacao",
    "pib_municipal": "ibge_pib_municipal",
    "pns_2013": "pns_2013",
    "pns_2019": "pns_2019",
}

# Fuso horário do Brasil (Brasília: UTC-3)
FUSO_BR = datetime.timezone(datetime.timedelta(hours=-3))


def _obter_metadados_fonte(pasta: str, nome_arquivo: str) -> tuple[str, str]:
    """Acha o nome e a descrição da fonte cruzando o nome do arquivo com o ID."""
    
    for trecho, fonte_id in MAPEAMENTO_ARQUIVO_ID.items():
        if trecho in nome_arquivo:
            fonte = FONTE_POR_ID.get(fonte_id)
            if fonte:
                return fonte.nome, fonte.descricao
            
    if pasta == "sinan":
        fonte_sinan = FONTE_POR_ID.get("sinan")
        if fonte_sinan:
            return fonte_sinan.nome, fonte_sinan.descricao
        
    return "(não mapeado no registro)", "(não mapeado no registro)"


def _montar_s3_filesystem() -> pafs.S3FileSystem:
    endpoint_sem_protocolo = env.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    esquema = "https" if env.MINIO_ENDPOINT.startswith("https://") else "http"
    return pafs.S3FileSystem(
        endpoint_override=endpoint_sem_protocolo,
        access_key=env.MINIO_ROOT_USER,
        secret_key=env.MINIO_ROOT_PASSWORD,
        scheme=esquema,
    )


def _contar_registros_parquet(s3_fs: pafs.S3FileSystem, bucket: str, key: str) -> int | None:
    try:
        caminho_s3 = f"{bucket}/{key}"
        pf = pq.ParquetFile(caminho_s3, filesystem=s3_fs)
        return pf.metadata.num_rows
    except Exception as e:
        print(f"[AVISO] Não consegui contar registros de {key}: {e}")
        return None


def gerar_linhas(s3_client, s3_fs, bucket: str) -> list[dict]:
    paginator = s3_client.get_paginator("list_objects_v2")
    linhas = []

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            nome_arquivo = key.rsplit("/", 1)[-1]

            if nome_arquivo in ("_manifest.json", NOME_ARQUIVO_SAIDA):
                continue

            # PA tem metadados próprio (sobe com o dataset dedicado do PA);
            # não entra no metadados geral.
            if key.startswith(PREFIXO_PA):
                continue

            pasta = key.split("/")[0] if "/" in key else ""

            num_registros = None
            if key.endswith(".parquet"):
                num_registros = _contar_registros_parquet(s3_fs, bucket, key)

            nome, descricao = _obter_metadados_fonte(pasta, nome_arquivo)

            linhas.append({
                "arquivo": key,
                "diretorio": pasta,
                "nome_fonte": nome,
                "descricao": descricao,
                "tamanho_bytes": obj["Size"],
                "num_registros": num_registros,
                "ultima_atualizacao": obj["LastModified"].astimezone(FUSO_BR).strftime("%Y-%m-%d %H:%M:%S"),
            })

    linhas.sort(key=lambda r: r["arquivo"])
    return linhas


def _competencia_do_parquet(nome_arquivo: str):
    """De 'producao_ambulatorial_202401.parquet' extrai (2024, 1)."""
    import re
    m = re.search(r"(\d{4})(\d{2})\.parquet$", nome_arquivo)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def gerar_metadados_pa(s3_client, s3_fs, bucket: str) -> tuple[list[dict], dict]:
    """Gera o metadados do PA: um breakdown POR ANO + um resumo agregado."""
    from collections import defaultdict

    por_ano_registros = defaultdict(int)
    por_ano_bytes = defaultdict(int)
    por_ano_competencias = defaultdict(set)
    por_ano_arquivos = defaultdict(int)
    por_ano_ultima_atualizacao = {}  # Rastreia a data do arquivo mais recente do ano

    total_arquivos = 0
    total_bytes = 0
    total_registros = 0
    competencias_todas = set()
    ultima_atualizacao_geral = None

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIXO_PA):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            nome = key.rsplit("/", 1)[-1]
            if not nome.endswith(".parquet"):
                continue  # ignora _manifest.json e afins
            comp = _competencia_do_parquet(nome)
            if not comp:
                continue
            ano, mes = comp
            nreg = _contar_registros_parquet(s3_fs, bucket, key) or 0
            lm = obj["LastModified"]

            por_ano_registros[ano] += nreg
            por_ano_bytes[ano] += obj["Size"]
            por_ano_competencias[ano].add((ano, mes))
            por_ano_arquivos[ano] += 1
            
            if ano not in por_ano_ultima_atualizacao or lm > por_ano_ultima_atualizacao[ano]:
                por_ano_ultima_atualizacao[ano] = lm

            total_arquivos += 1
            total_bytes += obj["Size"]
            total_registros += nreg
            competencias_todas.add((ano, mes))
            
            if ultima_atualizacao_geral is None or lm > ultima_atualizacao_geral:
                ultima_atualizacao_geral = lm

    def _comp_legivel(c):
        return f"{c[1]:02d}/{c[0]}"  # (ano, mes) -> "MM/AAAA"

    anos_ordenados = sorted(por_ano_registros)  # crescente, para calcular o acumulado corretamente
    acumulado = 0
    linhas_por_ano = {}
    for ano in anos_ordenados:
        acumulado += por_ano_registros[ano]
        comps = sorted(por_ano_competencias[ano])
        
        dt_utc = por_ano_ultima_atualizacao[ano]
        ult_atualizacao_ano_str = dt_utc.astimezone(FUSO_BR).strftime("%Y-%m-%d %H:%M:%S")

        linhas_por_ano[ano] = {
            "ano": ano,
            "num_arquivos": por_ano_arquivos[ano],
            "ultima_competencia": _comp_legivel(comps[-1]) if comps else "",
            "num_registros": por_ano_registros[ano],
            "registros_acumulado": acumulado,
            "ultima_atualizacao": ult_atualizacao_ano_str,
        }

    # exibe em ordem decrescente por ano
    linhas = [linhas_por_ano[ano] for ano in sorted(por_ano_registros, reverse=True)]

    comps_ord = sorted(competencias_todas)
    
    ult_atualizacao_resumo = ""
    if ultima_atualizacao_geral:
        ult_atualizacao_resumo = ultima_atualizacao_geral.astimezone(FUSO_BR).strftime("%Y-%m-%d %H:%M:%S")

    resumo = {
        "total_arquivos": total_arquivos,
        "total_competencias": len(competencias_todas),
        "total_registros": total_registros,
        "total_bytes": total_bytes,
        "primeira_competencia": f"{comps_ord[0][0]}{comps_ord[0][1]:02d}" if comps_ord else "",
        "ultima_competencia": f"{comps_ord[-1][0]}{comps_ord[-1][1]:02d}" if comps_ord else "",
        "anos_cobertos": f"{comps_ord[0][0]}-{comps_ord[-1][0]}" if comps_ord else "",
        "ultima_atualizacao": ult_atualizacao_resumo,
    }
    return linhas, resumo


def salvar_e_publicar_metadados_pa(s3_client, linhas: list[dict], resumo: dict, bucket: str):
    """Escreve o CSV do PA (breakdown por ano) e publica na pasta do PA."""
    if not linhas:
        print("[INFO] PA ainda não tem parquets publicados -- metadados do PA não gerado.")
        return

    def _gib(n):
        return f"{n / 1024**3:.2f} GiB"

    with open(CAMINHO_LOCAL_PA, "w", newline="", encoding="utf-8-sig") as f:
        colunas_pa = ["ano", "num_arquivos", "ultima_competencia", "num_registros", 
                      "registros_acumulado", "ultima_atualizacao"]
        writer = csv.DictWriter(f, fieldnames=colunas_pa)
        writer.writeheader()
        writer.writerows(linhas)

    s3_key = f"{PREFIXO_PA}{NOME_METADADOS_PA}"
    s3_client.upload_file(str(CAMINHO_LOCAL_PA), bucket, s3_key)
    print(f"✔ {NOME_METADADOS_PA} salvo e publicado em s3://{bucket}/{s3_key}")
    print(f"   {resumo['total_competencias']} competências | "
          f"{resumo['total_registros']:,} registros | {_gib(resumo['total_bytes'])} | "
          f"{resumo['anos_cobertos']}")


def main():
    s3_client = get_s3_client()
    s3_fs = _montar_s3_filesystem()

    # 1) metadados GERAL (tudo, exceto o PA)
    print(f"Listando bucket {env.MINIO_BUCKET} (exceto PA) e contando registros...")
    linhas = gerar_linhas(s3_client, s3_fs, env.MINIO_BUCKET)

    with open(CAMINHO_LOCAL_PERSISTENTE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLUNAS)
        writer.writeheader()
        writer.writerows(linhas)

    s3_client.upload_file(str(CAMINHO_LOCAL_PERSISTENTE), env.MINIO_BUCKET, NOME_ARQUIVO_SAIDA)

    nao_mapeados = [l["diretorio"] for l in linhas if l["nome_fonte"].startswith("(não mapeado")]
    if nao_mapeados:
        print(f"[AVISO] Pasta(s) sem Fonte correspondente no registro: {sorted(set(nao_mapeados))}")

    print(f"✔ {NOME_ARQUIVO_SAIDA} (geral) salvo e publicado na raiz do bucket "
          f"({len(linhas)} arquivo(s) catalogados, PA excluído).")

    # 2) metadados dedicado do PA (sobe DENTRO da pasta do PA)
    print(f"\nGerando metadados do PA (resumo + breakdown por ano)...")
    linhas_pa, resumo_pa = gerar_metadados_pa(s3_client, s3_fs, env.MINIO_BUCKET)
    salvar_e_publicar_metadados_pa(s3_client, linhas_pa, resumo_pa, env.MINIO_BUCKET)


if __name__ == "__main__":
    main()