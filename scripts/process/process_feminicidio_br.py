import os
import pandas as pd

# ------------------- Dicionário de CIDs -------------------
codigos_agressao = {
    # CID-10 X85-Y09 + Y35
    "X850": "Homicídio por disparo de arma de fogo",
    "X851": "Homicídio por arma branca",
    "X852": "Homicídio por envenenamento",
    "X853": "Homicídio por enforcamento, estrangulamento ou sufocação",
    "X854": "Homicídio por afogamento ou submersão",
    "X855": "Homicídio por explosivo",
    "X856": "Homicídio por incêndio, fogo ou chamas",
    "X857": "Homicídio por gases ou vapores",
    "X858": "Homicídio por objeto cortante ou perfurante",
    "X859": "Homicídio por outros meios X85",
    "X860": "Agressão por substâncias corrosivas",
    "X861": "Agressão por pesticidas ou produtos químicos",
    "X862": "Agressão por gás ou vapor tóxico",
    "X863": "Agressão por outro produto químico não especificado",
    "X864": "Agressão por envenenamento por drogas, medicamentos e substâncias biológicas",
    "X865": "Agressão por envenenamento por outras substâncias",
    "X866": "Agressão por envenenamento por substância não especificada",
    "X867": "Agressão por projétil de arma de fogo",
    "X868": "Agressão por espingarda, carabina ou arma de maior calibre",
    "X869": "Agressão por outro tipo de arma de fogo",
    "X870": "Agressão por disparo de arma de fogo de mão",
    "X871": "Agressão por espingarda, carabina ou arma de maior calibre",
    "X872": "Agressão por outro tipo de arma de fogo",
    "X873": "Agressão por explosivo",
    "X874": "Agressão por fumaça, fogo e chamas",
    "X875": "Agressão por objeto cortante",
    "X876": "Agressão por objeto contundente",
    "X877": "Agressão por enforcamento ou estrangulamento",
    "X878": "Agressão por afogamento",
    "X879": "Agressão por outro meio X87",
    "X880": "Agressão por gases e vapores",
    "X881": "Agressão por vapor de água quente",
    "X882": "Agressão por substância corrosiva",
    "X883": "Agressão por pesticida",
    "X884": "Agressão por outro produto químico",
    "X885": "Agressão por envenenamento por drogas, medicamentos e substâncias biológicas",
    "X886": "Agressão por envenenamento por outras substâncias",
    "X887": "Agressão por envenenamento por substância não especificada",
    "X888": "Agressão por projétil de arma de fogo",
    "X889": "Agressão por espingarda, carabina ou arma de maior calibre",
    "X890": "Agressão por objeto cortante ou perfurante",
    "X891": "Agressão por objeto contundente",
    "X892": "Agressão por estrangulamento ou sufocação",
    "X893": "Agressão por afogamento",
    "X894": "Agressão por fogo ou chamas",
    "X895": "Agressão por explosivo",
    "X896": "Agressão por armas de fogo",
    "X897": "Agressão por outros meios X89",
    "X898": "Agressão por substâncias químicas, local especificado",
    "X899": "Agressão com mecanismo não especificado",    
    "X900": "Agressão por produto químico não especificado",
    "X901": "Agressão por gás ou vapor tóxico",
    "X902": "Agressão por outro produto químico",
    "X903": "Agressão por envenenamento por drogas, medicamentos e substâncias biológicas",
    "X904": "Agressão por envenenamento por outras substâncias",
    "X905": "Agressão por envenenamento por substância não especificada",
    "X906": "Agressão por projétil de arma de fogo",
    "X907": "Disparo por espingarda ou carabina",
    "X908": "Disparo por outra arma de fogo",
    "X909": "Agressão por arma de fogo não especificada",
    "X910": "Agressão por enforcamento, estrangulamento ou sufocação",
    "X911": "Agressão por estrangulamento com corda ou fio",
    "X912": "Agressão por sufocação manual",
    "X913": "Agressão por outro meio de estrangulamento ou sufocação",
    "X914": "Agressão por enforcamento com outro meio",
    "X915": "Agressão por afogamento ou submersão",
    "X916": "Afogamento intencional em líquido",
    "X917": "Afogamento intencional em água",
    "X918": "Afogamento intencional em outro meio",
    "X919": "Afogamento intencional em meio não especificado",
    "X920": "Agressão por afogamento ou submersão",
    "X921": "Afogamento intencional em água",
    "X922": "Afogamento intencional em outro meio",
    "X923": "Afogamento intencional em meio não especificado",
    "X924": "Agressão por projétil de arma de fogo",
    "X925": "Disparo por espingarda ou carabina",
    "X926": "Disparo por outra arma de fogo",
    "X927": "Agressão por arma de fogo não especificada",
    "X928": "Agressão por projétil de arma de fogo",
    "X929": "Disparo por espingarda ou carabina",
    "X930": "Agressão por disparo de arma de fogo de mão",
    "X931": "Disparo de espingarda ou carabina",
    "X932": "Disparo por outra arma de fogo",
    "X933": "Agressão por arma de fogo não especificada",
    "X934": "Agressão por projétil de arma de fogo",
    "X935": "Disparo por espingarda ou carabina",
    "X936": "Agressão por disparo de arma de fogo de mão",
    "X937": "Disparo de espingarda ou carabina",
    "X938": "Disparo por outra arma de fogo",
    "X939": "Agressão por arma de fogo não especificada",
    "X940": "Disparo por outra arma de fogo",
    "X941": "Agressão por explosivo",
    "X942": "Agressão por fogo, chamas ou fumaça",
    "X943": "Agressão por incêndio proposital",
    "X944": "Agressão por fogo, chamas ou fumaça",
    "X945": "Agressão por incêndio proposital",
    "X946": "Agressão por vapor, líquidos quentes ou gases",
    "X947": "Agressão por objetos quentes",
    "X948": "Agressão por outros meios térmicos",
    "X949": "Agressão por outros meios térmicos",
    "X950": "Agressão por outro tipo de arma de fogo",
    "X951": "Agressão por disparo de arma de fogo de mão",
    "X952": "Disparo de espingarda ou carabina",
    "X953": "Disparo por outra arma de fogo",
    "X954": "Agressão por arma de fogo não especificada",
    "X955": "Agressão por projétil de arma de fogo",
    "X956": "Disparo por espingarda ou carabina",
    "X957": "Agressão por disparo de arma de fogo de mão",
    "X958": "Disparo de espingarda ou carabina",
    "X959": "Disparo por outra arma de fogo",
    "X960": "Agressão por explosivo",
    "X961": "Agressão por fogo, chamas ou fumaça",
    "X962": "Agressão por incêndio proposital",
    "X963": "Agressão por fogo, chamas ou fumaça",
    "X964": "Agressão por incêndio proposital",
    "X965": "Agressão por vapor, líquidos quentes ou gases",
    "X966": "Agressão por objetos quentes",
    "X967": "Agressão por outros meios térmicos",
    "X968": "Agressão por outros meios térmicos",
    "X969": "Agressão por explosivo",
    "X970": "Agressão por fogo, chamas ou fumaça",
    "X971": "Agressão por incêndio proposital",
    "X972": "Agressão por fogo, chamas ou fumaça",
    "X973": "Agressão por incêndio proposital",
    "X974": "Agressão por fogo, chamas ou fumaça",
    "X975": "Agressão por fogo, chamas ou fumaça",
    "X976": "Agressão por incêndio proposital",
    "X977": "Agressão por incêndio proposital",
    "X978": "Agressão por incêndio proposital",
    "X979": "Agressão por incêndio proposital",
    "X980": "Agressão por vapor, líquidos quentes ou gases",
    "X981": "Agressão por objetos quentes",
    "X982": "Agressão por outros meios térmicos",
    "X983": "Agressão por outros meios térmicos",
    "X984": "Agressão por projétil de arma de fogo",
    "X985": "Disparo por espingarda ou carabina",
    "X986": "Agressão por disparo de arma de fogo de mão",
    "X987": "Disparo de espingarda ou carabina",
    "X988": "Disparo por outra arma de fogo",
    "X989": "Agressão por arma de fogo não especificada",
    "X990": "Agressão por objeto cortante ou perfurante",
    "X991": "Agressão por objeto contundente",
    "X992": "Agressão por estrangulamento ou sufocação",
    "X993": "Agressão por afogamento",
    "X994": "Agressão por fogo ou chamas",
    "X995": "Agressão por explosivo",
    "X996": "Agressão por outros meios X99",
    "X997": "Agressão por outros meios físicos",
    "X998": "Agressão por outros meios químicos",
    "X999": "Agressão por meios não especificados",
    "Y000": "Agressão por arma de fogo não especificada",
    "Y001": "Agressão por objeto cortante",
    "Y002": "Agressão por objeto contundente",
    "Y003": "Agressão por afogamento",
    "Y004": "Agressão por envenenamento",
    "Y005": "Agressão sexual por força física",
    "Y006": "Negligência e abandono pelo cônjuge",
    "Y007": "Negligência e abandono pelos pais",
    "Y008": "Negligência e abandono por conhecido ou amigo",
    "Y009": "Negligência e abandono por pessoa não especificada",
    "Y010": "Agressão por projeção de lugar elevado",
    "Y011": "Agressão por queda de objeto em movimento",
    "Y012": "Agressão por explosão de gás ou vapor",
    "Y013": "Agressão por explosão de outro objeto",
    "Y014": "Agressão por impacto de veículo ferroviário",
    "Y015": "Agressão por impacto de veículo não motorizado",
    "Y016": "Agressão por impacto de objeto em movimento",
    "Y017": "Agressão por impacto de objeto fixo",
    "Y018": "Agressão por impacto de objeto cortante ou perfurante",
    "Y019": "Agressão por impacto de objeto contundente",
    "Y020": "Agressão por impacto de veículo a motor",
    "Y021": "Agressão por impacto de outro veículo",
    "Y022": "Agressão por impacto de veículo não especificado",
    "Y023": "Agressão por explosão de gás ou vapor",
    "Y024": "Agressão por explosão de outro objeto",
    "Y025": "Agressão por impacto de veículo ferroviário",
    "Y026": "Agressão por impacto de veículo não motorizado",
    "Y027": "Agressão por impacto de objeto em movimento",
    "Y028": "Agressão por impacto de objeto fixo",
    "Y029": "Agressão por impacto de objeto cortante ou perfurante",
    "Y030": "Agressão por força corporal",
    "Y031": "Agressão sexual não especificada",
    "Y032": "Agressão por outro meio físico",
    "Y033": "Agressão por outro meio químico",
    "Y034": "Agressão por outro meio mecânico",
    "Y035": "Agressão por outro meio desconhecido",
    "Y036": "Agressão por outro meio não especificado",
    "Y037": "Outras agressões físicas",
    "Y038": "Agressão sexual por outro meio",
    "Y039": "Agressão sexual",
    "Y040": "Outras agressões físicas",
    "Y041": "Agressão sexual por outro meio",
    "Y042": "Agressão sexual",
    "Y043": "Agressão sexual",
    "Y044": "Agressão sexual por outro meio",
    "Y045": "Agressão sexual",
    "Y046": "Agressão sexual por outro meio",
    "Y047": "Agressão sexual",
    "Y048": "Agressão sexual por outro meio",
    "Y049":  "Agressão sexual",
    "Y050": "Agressão sexual por força física",
    "Y051": "Agressão sexual por outro meio",
    "Y052": "Agressão sexual",
    "Y053": "Agressão sexual por outro meio",
    "Y054": "Agressão sexual",
    "Y055": "Agressão sexual por outro meio",
    "Y056": "Agressão sexual",
    "Y057": "Agressão sexual por outro meio",
    "Y058": "Agressão sexual",
    "Y059": "Agressão sexual por outro meio",
    "Y060": "Negligência e abandono pelo cônjuge",
    "Y061": "Negligência e abandono pelos pais",
    "Y062": "Negligência e abandono por conhecido",
    "Y063": "Negligência e abandono por outra pessoa",
    "Y064": "Negligência e abandono por pessoa não especificada",
    "Y065": "Síndromes de maus tratos pelo cônjuge",
    "Y066": "Síndromes de maus tratos pelos pais",
    "Y067": "Síndromes de maus tratos por conhecido",
    "Y068": "Síndromes de maus tratos por autoridade oficial",
    "Y069": "Síndromes de maus tratos por outra pessoa",
    "Y070": "Síndromes de maus tratos pelo cônjuge",
    "Y071": "Síndromes de maus tratos pelos pais",
    "Y072": "Síndromes de maus tratos por conhecido",
    "Y073": "Síndromes de maus tratos por autoridade oficial",
    "Y074": "Síndromes de maus tratos por outra pessoa",
    "Y075": "Síndromes de maus tratos por pessoa não especificada",
    "Y076": "Síndromes de maus tratos por pessoa não especificada",
    "Y077": "Síndromes de maus tratos por pessoa não especificada",
    "Y078": "Síndromes de maus tratos por pessoa não especificada",
    "Y079": "Síndromes de maus tratos por pessoa não especificada",
    "Y080": "Agressão por outros meios especificados",
    "Y081": "Agressão por outros meios físicos",
    "Y082": "Agressão por outros meios químicos",
    "Y083": "Agressão por outros meios mecânicos",
    "Y084": "Agressão por outros meios desconhecidos",
    "Y085": "Agressão por outros meios especificados",
    "Y086": "Agressão por outros meios físicos",
    "Y087": "Agressão por outros meios químicos",
    "Y088": "Agressão por outros meios mecânicos",
    "Y089": "Agressão por outros meios desconhecidos",
    "Y090": "Agressão por meios não especificados",
    "Y350": "Intervenção legal envolvendo uso de armas de fogo",
    "Y351": "Intervenção legal envolvendo uso de armas brancas",
    "Y352": "Intervenção legal envolvendo uso de força corporal",
    "Y353": "Intervenção legal envolvendo uso de outros meios",
    "Y354": "Intervenção legal envolvendo uso de meios não especificados",
    "Y355": "Intervenção legal envolvendo uso de armas de fogo",
    "Y356": "Intervenção legal envolvendo uso de armas brancas",
    "Y357": "Intervenção legal envolvendo uso de força corporal",
    "Y358": "Intervenção legal envolvendo uso de outros meios",
    "Y359": "Intervenção legal envolvendo uso de meios não especificados"
    
}

# ------------------- Mapeamentos -------------------

colunas_final = [
    "DT_NASCIMENTO", "DT_OBITO", "DT_CADASTRO_OBITO",
    "HORA_OBITO", "SEXO", "RACA_COR", "EST_CIVIL",
    "COD_MUNICIPIO_RESID", "COD_MUNICIPIO_OBITO", "LOCAL_OCORRENCIA_OBITO",
    "CAUSA_BASICA", "TIPO_OBITO", "DESCRICAO",
    "GESTANTE", "PUERPERIO"
]

mapa_sexo = {"1": "MASCULINO", "2": "FEMININO"}
mapa_raca = {"1": "BRANCA", "2": "PRETA", "3": "AMARELA", "4": "PARDA", "5": "INDIGENA"}
mapa_estciv = {"1": "SOLTEIRA", "2": "CASADA", "3": "VIUVA", "4": "DIVORCIADA", "5": "UNIÃO ESTAVEL"}
mapa_loccoro = {"1": "HOSPITAL", "2": "OUTROS ESTABELECIMENTOS DE SAUDE", "3": "DOMICILIO",
                "4": "VIA PUBLICA", "5": "OUTROS", "6": "ALDEIA INDIGENA"}
mapa_circobito = {"1": "ACIDENTE", "2": "SUICIDIO", "3": "HOMICIDIO", "4": "OUTROS"}
mapa_gestante = {"1": "SIM", "2": "NAO"}
mapa_puerperio = {"1": "SIM ATÉ 42 DIAS APOS O PARTO", "2": "SIM ATÉ 43 DIAS A 1 ANO APOS O PARTO", "3": "NAO"}


# ------------------- Função de filtro e mapeamento -------------------
def filtrar_e_mapear(df):
    
    df = df[df['SEXO'] == '2'].copy()
    df = df[df['CAUSABAS'].isin(codigos_agressao.keys())].copy()
    
    df.rename(columns={
        "DTNASC": "DT_NASCIMENTO",
        "DTOBITO": "DT_OBITO",
        "DTCADASTRO": "DT_CADASTRO_OBITO",
        "HORAOBITO": "HORA_OBITO",
        "SEXO": "SEXO",
        "RACACOR": "RACA_COR",
        "ESTCIV": "EST_CIVIL",
        "CODMUNRES": "COD_MUNICIPIO_RESID",
        "CODMUNOCOR": "COD_MUNICIPIO_OBITO",
        "LOCOCOR": "LOCAL_OCORRENCIA_OBITO",
        "CAUSABAS": "CAUSA_BASICA",
        "CIRCOBITO": "TIPO_OBITO",
        "OBITOGRAV": "GESTANTE",
        "OBITOPUERP": "PUERPERIO"
    }, inplace=True)
    
    df['SEXO'] = df['SEXO'].map(mapa_sexo).fillna("IGNORADO")
    df['RACA_COR'] = df['RACA_COR'].map(mapa_raca).fillna("IGNORADO")
    df['EST_CIVIL'] = df['EST_CIVIL'].map(mapa_estciv).fillna("IGNORADO")
    df['LOCAL_OCORRENCIA_OBITO'] = df['LOCAL_OCORRENCIA_OBITO'].map(mapa_loccoro).fillna("IGNORADO")
    df['TIPO_OBITO'] = df['TIPO_OBITO'].map(mapa_circobito).fillna("IGNORADO")
    df['GESTANTE'] = df['GESTANTE'].map(mapa_gestante).fillna("IGNORADO")
    df['PUERPERIO'] = df['PUERPERIO'].map(mapa_puerperio).fillna("IGNORADO")
    df['HORA_OBITO'] = df['HORA_OBITO'].apply(lambda x: "IGNORADO" if pd.isna(x) or str(x).strip() == "" else x)
    df['DT_NASCIMENTO'] = df['DT_NASCIMENTO'].apply(lambda x: "IGNORADO" if pd.isna(x) or str(x).strip() == "" else x)
    df['DT_CADASTRO_OBITO'] = df['DT_CADASTRO_OBITO'].apply(lambda x: "IGNORADO" if pd.isna(x) or str(x).strip() == "" else x)
    df['DESCRICAO'] = df['CAUSA_BASICA'].map(codigos_agressao)
    
    df = df[colunas_final]
    
    return df

# ------------------- Processamento -------------------

def processar_arquivo(caminho_arquivo, colunas_necessarias):
    """
    Lê um arquivo Parquet de forma otimizada (apenas colunas necessárias)
    e aplica a função de mapeamento e filtro.
    """
    print(f"Processando arquivo: {caminho_arquivo}")
    try:
        df = pd.read_parquet(
            caminho_arquivo,
            engine="fastparquet",
            columns=colunas_necessarias
        )
        print(f"Arquivo lido com sucesso. Shape: {df.shape}")

        df_processado = filtrar_e_mapear(df)
        print(f"Processamento concluído. Shape final: {df_processado.shape}")
        return df_processado

    except Exception as e:
        print(f"Ocorreu um erro ao processar o arquivo {caminho_arquivo}: {e}")
        raise

base_dir = "/opt/airflow" 
data_dir = os.path.join(base_dir, "data")
raw_dir = os.path.join(data_dir, "raw")
processed_dir = os.path.join(data_dir, "processed")
os.makedirs(processed_dir, exist_ok=True)

colunas_para_leitura = [
    "DTNASC", "DTOBITO", "DTCADASTRO", "HORAOBITO", "SEXO", "RACACOR",
    "ESTCIV", "CODMUNRES", "CODMUNOCOR", "LOCOCOR", "CAUSABAS", "CIRCOBITO",
    "OBITOGRAV", "OBITOPUERP"
]

caminho_sim = os.path.join(raw_dir, "raw_sim_causas_externas.parquet")
caminho_sim_prelim = os.path.join(raw_dir, "raw_sim_causas_externas_prelim.parquet")

df_fem_serie_historica = processar_arquivo(caminho_sim, colunas_para_leitura)
df_fem_prelim = processar_arquivo(caminho_sim_prelim, colunas_para_leitura)

# ------------------- Salvamento -------------------

caminho_saida_historica = os.path.join(processed_dir, "feminicidio_serie_historica.parquet")
caminho_saida_prelim = os.path.join(processed_dir, "feminicidio_prelim.parquet")

df_fem_serie_historica.to_parquet(caminho_saida_historica, engine="fastparquet", index=False)
df_fem_prelim.to_parquet(caminho_saida_prelim, engine="fastparquet", index=False)

print("\nArquivos salvos com sucesso em:", processed_dir)
print(f"- {os.path.basename(caminho_saida_historica)}")
print(f"- {os.path.basename(caminho_saida_prelim)}")