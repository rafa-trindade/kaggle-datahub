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
    "X890": "Agressão por objeto cortante ou perfurante",
    "X891": "Agressão por objeto contundente",
    "X892": "Agressão por estrangulamento ou sufocação",
    "X893": "Agressão por afogamento",
    "X894": "Agressão por fogo ou chamas",
    "X895": "Agressão por explosivo",
    "X896": "Agressão por armas de fogo",
    "X897": "Agressão por outros meios X89",
    "X900": "Agressão por produto químico não especificado",
    "X901": "Agressão por gás ou vapor tóxico",
    "X902": "Agressão por outro produto químico",
    "X910": "Agressão por enforcamento, estrangulamento ou sufocação",
    "X911": "Agressão por estrangulamento com corda ou fio",
    "X912": "Agressão por sufocação manual",
    "X920": "Agressão por afogamento ou submersão",
    "X921": "Afogamento intencional em água",
    "X930": "Agressão por disparo de arma de fogo de mão",
    "X931": "Disparo de espingarda ou carabina",
    "X940": "Disparo por outra arma de fogo",
    "X950": "Agressão por outro tipo de arma de fogo",
    "X960": "Agressão por explosivo",
    "X970": "Agressão por fogo, chamas ou fumaça",
    "X971": "Agressão por incêndio proposital",
    "X980": "Agressão por vapor, líquidos quentes ou gases",
    "X981": "Agressão por objetos quentes",
    "X990": "Agressão por objeto cortante ou perfurante",
    "X991": "Agressão por objeto contundente",
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
    "Y020": "Agressão por impacto de veículo a motor",
    "Y021": "Agressão por impacto de outro veículo",
    "Y030": "Agressão por força corporal",
    "Y031": "Agressão sexual não especificada",
    "Y040": "Outras agressões físicas",
    "Y041": "Agressão sexual por outro meio",
    "Y050": "Agressão sexual por força física",
    "Y060": "Negligência e abandono pelo cônjuge",
    "Y061": "Negligência e abandono pelos pais",
    "Y062": "Negligência e abandono por conhecido",
    "Y063": "Negligência e abandono por outra pessoa",
    "Y064": "Negligência e abandono por pessoa não especificada",
    "Y070": "Síndromes de maus tratos pelo cônjuge",
    "Y071": "Síndromes de maus tratos pelos pais",
    "Y072": "Síndromes de maus tratos por conhecido",
    "Y073": "Síndromes de maus tratos por autoridade oficial",
    "Y074": "Síndromes de maus tratos por outra pessoa",
    "Y075": "Síndromes de maus tratos por pessoa não especificada",
    "Y080": "Agressão por outros meios especificados",
    "Y081": "Agressão por outros meios físicos",
    "Y082": "Agressão por outros meios químicos",
    "Y083": "Agressão por outros meios mecânicos",
    "Y084": "Agressão por outros meios desconhecidos",
    "Y090": "Agressão por meios não especificados",
    "Y350": "Intervenção legal envolvendo uso de armas de fogo"
    
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