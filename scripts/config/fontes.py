"""
Registro central de todas as fontes de dados do pipeline.

Cada fonte publica em subpasta própria (bucket e dataset Kaggle).
Dados não persistem localmente -- processados e publicados no bucket.
run_all.py e load_*.py pegam automaticamente as fontes registradas.
"""
from dataclasses import dataclass, field

DATASET_KAGGLE = "brazilian-kaggle-datahub"
# ver scripts/common/bucket_sync.py


@dataclass(frozen=True)
class Fonte:
    id: str
    nome: str
    descricao: str
    tipo: str  # "pipeline" | "pipeline_manual" | "manual"
    pasta_bucket: str  # subpasta dentro do bucket/dataset publicado, ex: "cnes/estabelecimentos"
    extract_modules: list[str] = field(default_factory=list)
    process_modules: list[str] = field(default_factory=list)
    nota: str = ""


FONTES: list[Fonte] = [
    # ------------------------------------------------------------------
    # CNES (Estabelecimentos, Habilitações, Leitos -- sem filtro de
    # especialidade, todas as áreas)
    # ------------------------------------------------------------------
    Fonte(
        id="cnes_estabelecimentos",
        nome="CNES - Estabelecimentos de Saúde",
        descricao="Cadastro Nacional de Estabelecimentos de Saúde -- identificação, endereço, CNPJ e infraestrutura de todos os estabelecimentos de saúde do Brasil.",
        tipo="pipeline",
        pasta_bucket="cnes",
        extract_modules=["scripts.extract.dados_abertos.fetch_cnes_estabelecimentos"],
        process_modules=["scripts.process.dados_abertos.process_cnes_estabelecimentos"],
    ),
    Fonte(
        id="cnes_habilitacoes",
        nome="CNES - Habilitações",
        descricao="Habilitações de todos os estabelecimentos de saúde do Brasil.",
        tipo="pipeline",
        pasta_bucket="cnes",
        extract_modules=["scripts.extract.datasus.fetch_cnes_habilitacao"],
        process_modules=["scripts.process.datasus.process_cnes_habilitacao"],
    ),
    Fonte(
        id="cnes_leitos",
        nome="CNES - Leitos",
        descricao="Contagem de leitos de todos os estabelecimentos de saúde do Brasil.",
        tipo="pipeline",
        pasta_bucket="cnes",
        extract_modules=["scripts.extract.datasus.fetch_cnes_leitos"],
        process_modules=["scripts.process.datasus.process_cnes_leitos"],
    ),
    Fonte(
        id="cnes_profissionais",
        nome="CNES - Profissionais",
        descricao="Profissionais de saúde cadastrados por estabelecimento (CBO, carga horária, forma de contratação).",
        tipo="pipeline",
        pasta_bucket="cnes",
        extract_modules=["scripts.extract.datasus.fetch_cnes_profissionais"],
        process_modules=["scripts.process.datasus.process_cnes_profissionais"],
    ),
    Fonte(
        id="cnes_equipamentos",
        nome="CNES - Equipamentos",
        descricao="Equipamentos cadastrados por estabelecimento (raio-X, ressonância, tomógrafo etc).",
        tipo="pipeline",
        pasta_bucket="cnes",
        extract_modules=["scripts.extract.datasus.fetch_cnes_equipamentos"],
        process_modules=["scripts.process.datasus.process_cnes_equipamentos"],
    ),

    # ------------------------------------------------------------------
    # SIM -- Declarações de Óbito (geral, todas as causas) e Causas
    # Externas (sistema separado do DATASUS)
    # ------------------------------------------------------------------
    Fonte(
        id="sim_do_cid9",
        nome="SIM - Declaração de Óbito, CID-9 (1979-1995)",
        descricao="Óbitos por todas as causas, era CID-9.",
        tipo="pipeline",
        pasta_bucket="sim",
        extract_modules=["scripts.extract.datasus.fetch_sim_declaracao_obito_cid9"],
        process_modules=["scripts.process.datasus.process_sim_declaracao_obito_cid9"],
    ),
    Fonte(
        id="sim_do_cid10",
        nome="SIM - Declaração de Óbito, CID-10 (1996-atual)",
        descricao="Óbitos por todas as causas, era CID-10.",
        tipo="pipeline",
        pasta_bucket="sim",
        extract_modules=["scripts.extract.datasus.fetch_sim_declaracao_obito_cid10"],
        process_modules=["scripts.process.datasus.process_sim_declaracao_obito_cid10"],
    ),
    Fonte(
        id="sim_causas_externas_cid9",
        nome="SIM - Causas Externas, CID-9 (1979-1995)",
        descricao="Óbitos por causas externas (acidentes, violência), era CID-9",
        tipo="pipeline",
        pasta_bucket="sim",
        extract_modules=["scripts.extract.datasus.fetch_sim_causas_externas_cid9"],
        process_modules=["scripts.process.datasus.process_sim_causas_externas_cid9"],
    ),
    Fonte(
        id="sim_causas_externas_cid10",
        nome="SIM - Causas Externas, CID-10 (1996-atual)",
        descricao="Óbitos por causas externas (acidentes, violência), era CID-10.",
        tipo="pipeline",
        pasta_bucket="sim",
        extract_modules=["scripts.extract.datasus.fetch_sim_causas_externas_cid10"],
        process_modules=["scripts.process.datasus.process_sim_causas_externas_cid10"],
    ),
    Fonte(
        id="sim_dofet_cid9",
        nome="SIM - Óbitos Fetais, CID-9 (1979-1995)",
        descricao="Declarações de Óbito Fetal, era CID-9.",
        tipo="pipeline",
        pasta_bucket="sim",
        extract_modules=["scripts.extract.datasus.fetch_sim_dofet_cid9"],
        process_modules=["scripts.process.datasus.process_sim_dofet_cid9"],
    ),
    Fonte(
        id="sim_dofet_cid10",
        nome="SIM - Óbitos Fetais, CID-10 (1996-atual)",
        descricao="Declarações de Óbito Fetal, era CID-10.",
        tipo="pipeline",
        pasta_bucket="sim",
        extract_modules=["scripts.extract.datasus.fetch_sim_dofet_cid10"],
        process_modules=["scripts.process.datasus.process_sim_dofet_cid10"],
    ),
    Fonte(
        id="sim_doinf_cid9",
        nome="SIM - Óbitos Infantis, CID-9 (1979-1995)",
        descricao="Declarações de Óbito Infantil, era CID-9.",
        tipo="pipeline",
        pasta_bucket="sim",
        extract_modules=["scripts.extract.datasus.fetch_sim_doinf_cid9"],
        process_modules=["scripts.process.datasus.process_sim_doinf_cid9"],
    ),
    Fonte(
        id="sim_doinf_cid10",
        nome="SIM - Óbitos Infantis, CID-10 (1996-atual)",
        descricao="Declarações de Óbito Infantil, era CID-10.",
        tipo="pipeline",
        pasta_bucket="sim",
        extract_modules=["scripts.extract.datasus.fetch_sim_doinf_cid10"],
        process_modules=["scripts.process.datasus.process_sim_doinf_cid10"],
    ),
    Fonte(
        id="sim_domat",
        nome="SIM - Óbitos Maternos (1996-atual)",
        descricao="Declarações de Óbito Materno. Só existe a partir de 1996 (sem era CID-9).",
        tipo="pipeline",
        pasta_bucket="sim",
        extract_modules=["scripts.extract.datasus.fetch_sim_domat"],
        process_modules=["scripts.process.datasus.process_sim_domat"],
    ),
    Fonte(
        id="sim_dorext",
        nome="SIM - Mortalidade de Residentes no Exterior (2013-atual)",
        descricao="Brasileiros falecidos no exterior. Só existe a partir de 2013 (sem era CID-9)..",
        tipo="pipeline",
        pasta_bucket="sim",
        extract_modules=["scripts.extract.datasus.fetch_sim_dorext"],
        process_modules=["scripts.process.datasus.process_sim_dorext"],
    ),

    # ------------------------------------------------------------------
    # SINASC -- nascidos vivos, mesma infra do SIM
    # ------------------------------------------------------------------
    Fonte(
        id="sinasc",
        nome="SINASC - Nascidos Vivos",
        descricao="Declarações de Nascido Vivo, série histórica (inclui período antigo)",
        tipo="pipeline",
        pasta_bucket="sinasc",
        extract_modules=["scripts.extract.datasus.fetch_sinasc"],
        process_modules=["scripts.process.datasus.process_sinasc"],
    ),
    Fonte(
        id="sinasc_dnex",
        nome="SINASC - Nascidos Vivos no Exterior (DNEX)",
        descricao="Brasileiros nascidos no exterior, registrados no sistema (arquivo final separado, não pertence a nenhuma UF).",
        tipo="pipeline",
        pasta_bucket="sinasc",
        extract_modules=["scripts.extract.datasus.fetch_sinasc_dnex"],
        process_modules=["scripts.process.datasus.process_sinasc_dnex"],
    ),

    # ------------------------------------------------------------------
    # SIH/SUS -- internações hospitalares, série moderna (2008-atual)
    # ------------------------------------------------------------------
    Fonte(
        id="sih_rd",
        nome="SIH/SUS - AIH Reduzida",
        descricao="Internações hospitalares aprovadas para pagamento pelo SUS, 2008-atual. Diagnósticos, procedimentos, valores, tempo de permanência.",
        tipo="pipeline",
        pasta_bucket="sih",
        extract_modules=["scripts.extract.datasus.fetch_sih_rd"],
        process_modules=["scripts.process.datasus.process_sih_rd"],
    ),
    Fonte(
        id="sih_rj",
        nome="SIH/SUS - AIH Rejeitadas",
        descricao="Internações hospitalares rejeitadas para pagamento pelo SUS, 2008-atual.",
        tipo="pipeline",
        pasta_bucket="sih",
        extract_modules=["scripts.extract.datasus.fetch_sih_rj"],
        process_modules=["scripts.process.datasus.process_sih_rj"],
    ),
    Fonte(
        id="sih_sp",
        nome="SIH/SUS - Serviços Profissionais",
        descricao="Atos médicos realizados durante internações hospitalares do SUS, 2008-atual.",
        tipo="pipeline",
        pasta_bucket="sih",
        extract_modules=["scripts.extract.datasus.fetch_sih_sp"],
        process_modules=["scripts.process.datasus.process_sih_sp"],
    ),

    # ------------------------------------------------------------------
    # SINAN -- doenças e agravos de notificação compulsória, 58 agravos
    # configurados (ver scripts/config/agravos_sinan.py), cada um
    # publicado como parquet próprio em sinan/
    # ------------------------------------------------------------------
    Fonte(
        id="sinan",
        nome="SINAN - Doenças e Agravos de Notificação Compulsória",
        descricao="58 agravos (dengue, tuberculose, violência interpessoal, sífilis, AIDS/HIV, hanseníase e outros)",
        tipo="pipeline",
        pasta_bucket="sinan",
        extract_modules=["scripts.extract.datasus.fetch_sinan"],
        process_modules=["scripts.process.datasus.process_sinan"],
    ),

    # ------------------------------------------------------------------
    # SIA/SUS -- Sistema de Informações Ambulatoriais.
    # ------------------------------------------------------------------
    Fonte(
        id="sia_pa",
        nome="SIA/SUS - Produção Ambulatorial",
        descricao="Produção ambulatorial do SUS (BPA) -- procedimentos, quantidades e valores por estabelecimento e competência. Maior base do DATASUS, série desde Jul/1994.",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_pa"],
        process_modules=["scripts.process.datasus.process_sia_pa"],
    ),
    Fonte(
        id="sia_am",
        nome="SIA/SUS - APAC de Medicamentos",
        descricao="APACs de medicamentos (componente especializado da assistência farmacêutica), 2008-atual.",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_am"],
        process_modules=["scripts.process.datasus.process_sia_am"],
    ),
    Fonte(
        id="sia_aq",
        nome="SIA/SUS - APAC de Quimioterapia",
        descricao="APACs de quimioterapia oncológica, 2008-atual.",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_aq"],
        process_modules=["scripts.process.datasus.process_sia_aq"],
    ),
    Fonte(
        id="sia_ar",
        nome="SIA/SUS - APAC de Radioterapia",
        descricao="APACs de radioterapia oncológica, 2008-atual.",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_ar"],
        process_modules=["scripts.process.datasus.process_sia_ar"],
    ),
    Fonte(
        id="sia_atd",
        nome="SIA/SUS - APAC de Tratamento Dialítico",
        descricao="APACs de tratamento dialítico (diálise), Jun/2014-atual.",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_atd"],
        process_modules=["scripts.process.datasus.process_sia_atd"],
    ),
    Fonte(
        id="sia_an",
        nome="SIA/SUS - APAC de Nefrologia",
        descricao="APACs de nefrologia, 2008 a Out/2014 (substituída pela ATD).",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_an"],
        process_modules=["scripts.process.datasus.process_sia_an"],
    ),
    Fonte(
        id="sia_ad",
        nome="SIA/SUS - APAC de Laudos Diversos",
        descricao="APACs de laudos diversos, 2008-atual.",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_ad"],
        process_modules=["scripts.process.datasus.process_sia_ad"],
    ),
    Fonte(
        id="sia_ps",
        nome="SIA/SUS - Psicossocial (RAAS)",
        descricao="RAAS Psicossocial -- atendimentos da Rede de Atenção Psicossocial (CAPS), Jan/2013-atual.",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_ps"],
        process_modules=["scripts.process.datasus.process_sia_ps"],
    ),
    Fonte(
        id="sia_sad",
        nome="SIA/SUS - Atenção Domiciliar (RAAS)",
        descricao="RAAS de Atenção Domiciliar (SAD/Melhor em Casa), Nov/2012-atual.",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_sad"],
        process_modules=["scripts.process.datasus.process_sia_sad"],
    ),
    Fonte(
        id="sia_acf",
        nome="SIA/SUS - APAC de Confecção de Fístula Arteriovenosa",
        descricao="APACs de confecção de fístula arteriovenosa (acesso para diálise), Jun/2014-atual.",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_acf"],
        process_modules=["scripts.process.datasus.process_sia_acf"],
    ),
    Fonte(
        id="sia_ab",
        nome="SIA/SUS - APAC de Cirurgia Bariátrica",
        descricao="APACs de acompanhamento a cirurgia bariátrica, 2008 a Mar/2013 (substituída pela ABO).",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_ab"],
        process_modules=["scripts.process.datasus.process_sia_ab"],
    ),
    Fonte(
        id="sia_abo",
        nome="SIA/SUS - APAC de Acompanhamento Pós Cirurgia Bariátrica",
        descricao="APACs de acompanhamento pós cirurgia bariátrica, Abr/2013-atual.",
        tipo="pipeline",
        pasta_bucket="sia",
        extract_modules=["scripts.extract.datasus.fetch_sia_abo"],
        process_modules=["scripts.process.datasus.process_sia_abo"],
    ),

    # ------------------------------------------------------------------
    # CIHA -- Comunicação de Informação Hospitalar e Ambulatorial
    # (série acumulativa a partir de Jan/2011; sucede o antigo CIH)
    # ------------------------------------------------------------------
    Fonte(
        id="ciha",
        nome="CIHA - Comunicação de Informação Hospitalar e Ambulatorial",
        descricao="Internações e atendimentos ambulatoriais comunicados ao SUS, incluindo produção não-SUS (particular, planos de saúde), 2011-atual. Sucede o antigo CIH.",
        tipo="pipeline",
        pasta_bucket="ciha",
        extract_modules=["scripts.extract.datasus.fetch_ciha"],
        process_modules=["scripts.process.datasus.process_ciha"],
    ),

    # ------------------------------------------------------------------
    # Geo / Macrorregião
    # ------------------------------------------------------------------
    Fonte(
        id="macroregiao",
        nome="Macrorregiões de Saúde (geolocalização)",
        descricao="Municípios brasileiros associados às macrorregiões de saúde, regiões de saúde e coordenadas geográficas.",
        tipo="pipeline",
        pasta_bucket="geo",
        extract_modules=["scripts.extract.dados_abertos.fetch_macroregiao_de_saude"],
        process_modules=["scripts.process.dados_abertos.process_macroregiao_de_saude"],
    ),

    # ------------------------------------------------------------------
    # IBGE -- população estimada (novo)
    # ------------------------------------------------------------------
    Fonte(
        id="ibge_populacao",
        nome="IBGE - População Estimada por Município",
        descricao="Série de população estimada por município e ano -- habilita taxas per capita sobre qualquer outra fonte deste hub.",
        tipo="pipeline",
        pasta_bucket="ibge",
        extract_modules=["scripts.extract.ibge.fetch_populacao_estimada"],
        process_modules=["scripts.process.ibge.process_populacao_estimada"],
    ),
    Fonte(
        id="ibge_pib_municipal",
        nome="IBGE - PIB Municipal",
        descricao="PIB, Valor Adicionado Bruto por setor (Agropecuária/Indústria/Serviços) e atividade econômica, por município e ano (tabela SIDRA 5938).",
        tipo="pipeline",
        pasta_bucket="ibge",
        extract_modules=["scripts.extract.ibge.fetch_pib_municipal"],
        process_modules=["scripts.process.ibge.process_pib_municipal"],
    ),

    # ------------------------------------------------------------------
    # PNS -- microdados brutos, sem recorte temático
    # ------------------------------------------------------------------
    Fonte(
        id="pns_2013",
        nome="PNS/IBGE 2013 - Microdados Brutos",
        descricao="Microdados de posição fixa completos, sem recorte temático.",
        tipo="pipeline_manual",
        pasta_bucket="ibge",
        process_modules=["scripts.process.ibge.process_pns_2013_bruto"],
        nota="Microdado bruto (PNS_2013.txt) baixado manualmente do IBGE, colocado em data/landing/ibge/.",
    ),
    Fonte(
        id="pns_2019",
        nome="PNS/IBGE 2019 - Microdados Brutos",
        descricao="Microdados de posição fixa completos, sem recorte temático.",
        tipo="pipeline_manual",
        pasta_bucket="ibge",
        process_modules=["scripts.process.ibge.process_pns_2019_bruto"],
        nota="Microdado bruto (PNS_2019.txt) baixado manualmente do IBGE, colocado em data/landing/ibge/.",
    ),
]


def get_fonte(id: str) -> Fonte:
    for f in FONTES:
        if f.id == id:
            return f
    raise KeyError(f"Fonte '{id}' não registrada em scripts/config/fontes.py")