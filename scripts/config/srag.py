"""
SRAG - Síndrome Respiratória Aguda Grave (SIVEP-Gripe), via Portal de
Dados Abertos do Ministério da Saúde (antigo OpenDataSUS):

    https://dadosabertos.saude.gov.br/dataset?q=srag

O Ministério publica um arquivo por ano (INFLUDAA), agrupados em três
conjuntos que correspondem a três versões da ficha de notificação --
por isso o hub publica um parquet por era (estruturas de campos
diferentes entre eras, iguais dentro de cada era):

  srag_2009_2012.parquet   -> conjunto "srag-2009-2012"   (CSV)
  srag_2013_2018.parquet   -> conjunto "srag-2013-2018"   (CSV)
  srag_2019_atual.parquet  -> conjunto "srag-2019-a-AAAA" (Parquet/CSV,
                              "banco vivo": anos recentes são republicados
                              periodicamente com nome datado, ex.
                              INFLUD25-28-09-2026.parquet)

Os nomes dos conjuntos e dos arquivos são descobertos dinamicamente a
cada execução (o slug do conjunto vigente muda a cada ano, ex.
srag-2019-a-2026 -> srag-2019-a-2027).
"""

PORTAL_SAUDE = "https://dadosabertos.saude.gov.br"
TERMO_BUSCA = "srag"

PASTA_BUCKET = "srag"


# (nome_arquivo_final_sem_extensao, ano_inicial, ano_final_ou_None)
ERAS_SRAG = [
    ("srag_2009_2012", 2009, 2012),
    ("srag_2013_2018", 2013, 2018),
    ("srag_2019_atual", 2019, None),
]


def era_do_ano(ano: int) -> str | None:
    for nome, inicio, fim in ERAS_SRAG:
        if ano >= inicio and (fim is None or ano <= fim):
            return nome
    return None
