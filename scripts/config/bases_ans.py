"""
Bases da ANS (Agência Nacional de Saúde Suplementar) importadas da
base TabNet/TabWin, distribuída em .dbc (mesmo formato do DATASUS):

    https://dadosabertos.ans.gov.br/FTP/Base_de_dados/Microdados/dados_dbc/

Página oficial que aponta para essa base:
    https://www.gov.br/ans/pt-br/acesso-a-informacao/perfil-do-setor/dados-e-indicadores-do-setor/baixar-base-de-dados

Cada tabela vira um parquet próprio em ans/ (estruturas diferentes entre
si). Todas compartilham o manifesto ans/_manifest.json -- os prefixos de
arquivo (tb_bb_, tb_br_, ...) são distintos, então não há colisão.

modo:
  "incremental"  -> série que acumula (um arquivo por mês/trimestre/ano);
                    arquivos novos ou revisados (tamanho diferente) são
                    mesclados ao parquet já publicado, os demais são pulados.
  "retrato"      -> a ANS só mantém o arquivo mais recente no servidor;
                    cada versão nova substitui por completo a anterior
                    (mesma mecânica do CNES).
"""
from dataclasses import dataclass

URL_BASE_ANS = "https://dadosabertos.ans.gov.br/FTP/Base_de_dados/Microdados/dados_dbc"

# Arquivos .def/.cnv do TabWin -- necessários para decodificar os códigos
# das tabelas abaixo (faixa etária, tipo de contratação, modalidade etc).
URL_AUXILIARES_ANS = "https://dadosabertos.ans.gov.br/FTP/Base_de_dados/Microdados/arquivos_auxiliares_de_tab_def_e_cnv/"


@dataclass(frozen=True)
class BaseANS:
    nome_arquivo: str      # nome do parquet final (sem extensão), em ans/
    subpasta: str          # subpasta dentro de dados_dbc/
    prefixo: str           # prefixo dos .dbc (com underscore final, ex: "tb_bb_")
    modo: str              # "incremental" | "retrato"
    nome: str
    descricao: str


BASES_ANS: list[BaseANS] = [
    # ------------------------------------------------------------------
    # Beneficiários
    # ------------------------------------------------------------------
    BaseANS(
        nome_arquivo="beneficiarios_por_municipio",
        subpasta="beneficiarios/municipios",
        prefixo="tb_bb_",
        modo="incremental",
        nome="ANS - Beneficiários por Município",
        descricao="Beneficiários de planos de saúde por município, operadora, faixa etária, sexo e tipo de plano. Trimestral, Mar/2000-atual.",
    ),
    BaseANS(
        nome_arquivo="beneficiarios_por_uf_regiao_metropolitana_capital",
        subpasta="beneficiarios/uf_regiao_metropolitana_e_capital",
        prefixo="tb_br_",
        modo="incremental",
        nome="ANS - Beneficiários por UF, Região Metropolitana e Capital",
        descricao="Beneficiários de planos de saúde agregados por UF, região metropolitana e capital. Trimestral, Mar/2000-atual.",
    ),
    BaseANS(
        nome_arquivo="beneficiarios_por_operadora",
        subpasta="beneficiarios/operadoras",
        prefixo="tb_cc_",
        modo="incremental",
        nome="ANS - Beneficiários por Operadora",
        descricao="Beneficiários de planos de saúde por operadora. Trimestral, Jun/2011-atual.",
    ),
    BaseANS(
        nome_arquivo="taxa_de_cobertura",
        subpasta="beneficiarios/taxa_cobertura",
        prefixo="tb_tx_",
        modo="retrato",
        nome="ANS - Taxa de Cobertura de Planos de Saúde",
        descricao="Taxa de cobertura de planos de saúde (beneficiários / população). Retrato da competência mais recente publicada pela ANS.",
    ),
    BaseANS(
        nome_arquivo="mortalidade_por_operadora",
        subpasta="beneficiarios/mortalidade_por_operadora",
        prefixo="tb_mm_",
        modo="incremental",
        nome="ANS - Mortalidade por Operadora (2004-2009)",
        descricao="Série histórica descontinuada (2004-2009) da pasta 'mortalidade_por_operadora' da base TabNet da ANS, por operadora. Consultar a documentação TabWin da ANS para o significado de cada campo.",
    ),

    # ------------------------------------------------------------------
    # Ressarcimento ao SUS -- atendimentos no SUS de beneficiários de planos
    # ------------------------------------------------------------------
    BaseANS(
        nome_arquivo="ressarcimento_ao_sus",
        subpasta="ressarcimento_ao_sus",
        prefixo="tb_res_",
        modo="incremental",
        nome="ANS - Ressarcimento ao SUS",
        descricao="Atendimentos realizados no SUS a beneficiários de planos de saúde, identificados para ressarcimento. Anual, 2001-atual (anos recentes são revisados).",
    ),

    # ------------------------------------------------------------------
    # Operadoras
    # ------------------------------------------------------------------
    BaseANS(
        nome_arquivo="operadoras_ativas",
        subpasta="operadoras/oper_com_registro_ativo",
        prefixo="tb_opa_",
        modo="retrato",
        nome="ANS - Operadoras com Registro Ativo",
        descricao="Cadastro das operadoras de planos de saúde com registro ativo na ANS. Retrato mais recente.",
    ),
    BaseANS(
        nome_arquivo="receitas_e_despesas_operadoras",
        subpasta="operadoras/receita_de_contraprest_e_despesas",
        prefixo="tb_rc_",
        modo="incremental",
        nome="ANS - Receita de Contraprestações e Despesas das Operadoras",
        descricao="Receitas de contraprestações e despesas assistenciais das operadoras. Anual, 2001-atual.",
    ),

    # ------------------------------------------------------------------
    # Planos
    # ------------------------------------------------------------------
    BaseANS(
        nome_arquivo="planos",
        subpasta="planos",
        prefixo="tb_pl_",
        modo="retrato",
        nome="ANS - Planos de Saúde",
        descricao="Planos de saúde registrados na ANS e suas características. Retrato mais recente.",
    ),

    # ------------------------------------------------------------------
    # Demandas dos consumidores
    # ------------------------------------------------------------------
    BaseANS(
        nome_arquivo="demandas_reclamacoes",
        subpasta="demandas/reclamacoes",
        prefixo="tb_rec_",
        modo="incremental",
        nome="ANS - Demandas de Consumidores: Reclamações",
        descricao="Reclamações de consumidores registradas na ANS. Mensal, Jan/2010-atual.",
    ),
    BaseANS(
        nome_arquivo="demandas_nip",
        subpasta="demandas/nip",
        prefixo="tb_nip_",
        modo="incremental",
        nome="ANS - Demandas de Consumidores: NIP",
        descricao="Demandas tratadas via Notificação de Intermediação Preliminar (NIP). Mensal, Jan/2011-atual.",
    ),
    BaseANS(
        nome_arquivo="demandas_informacoes",
        subpasta="demandas/informacoes",
        prefixo="tb_inf_",
        modo="incremental",
        nome="ANS - Demandas de Consumidores: Pedidos de Informação",
        descricao="Pedidos de informação de consumidores registrados na ANS. Mensal, Jan/2010-atual.",
    ),
]


def get_base_ans_por_arquivo(nome_arquivo_parquet: str) -> BaseANS | None:
    """Acha a base pelo nome do parquet publicado (ex: 'planos.parquet')."""
    nome = nome_arquivo_parquet.rsplit("/", 1)[-1].removesuffix(".parquet")
    for b in BASES_ANS:
        if b.nome_arquivo == nome:
            return b
    return None
