import pandas as pd

base_dir = "/opt/airflow" 
data_dir = os.path.join(base_dir, "data")
microdados_dir = os.path.join(base_dir, "utils", "microdados_ibge")
processed_dir = os.path.join(data_dir, "processed")
os.makedirs(processed_dir, exist_ok=True)

colunas_posicoes = {
    "V0001": (0, 2), 
    "V0026": (37, 38),  
    "C006": (105, 106),
    "C008": (114, 117),
    "C009": (117, 118),    
    "O025": (606, 607),  
    "O027": (607, 608), 
    "O028": (608, 609),  
    "O029": (609, 610),  
    "O030": (610, 611),
    "O031": (611, 612),
    "O032": (612, 613),
    "O033": (613, 614),
    "O034": (614, 616),
    "O035": (616, 617),
    "O036": (617, 618),
    "O037": (618, 619),
    "O038": (619, 620),
    "O039": (620, 621),
    "O040": (621, 622),
    "O041": (622, 623),
    "O042": (623, 625),  
    "O043": (625, 626),
    "O044": (626, 627),
    "O045": (627, 628),
    "O046": (628, 630),  
    "O047": (630, 631),
    "O048": (631, 632)
}

colunas_nomes = {
    "V0001": "Unidade da Federação",
    "V0026": "Área",
    "C006": "Sexo",
    "C008": "Idade",
    "C009": "Cor ou raça",
    "O025": "Vítima sofreu violência",
    "O027": "Tipo de violência",
    "O028": "Meio de violência",
    "O029": "Local da violência",
    "O030": "Autor da violência",
    "O031": "Vitima buscou ajuda",
    "O032": "Vitima recebeu atendimento",
    "O033": "Vitima registrou ocorrência",
    "O034": "Tipo de serviço de saúde procurado",
    "O035": "Recebeu acompanhamento psicológico",
    "O036": "Recebeu atendimento social",
    "O037": "Recebeu acompanhamento legal",
    "O038": "Frequência da violência",
    "O039": "Tipo de violência sofrida",
    "O040": "Meio utilizado na violência",
    "O041": "Local da agressão",
    "O042": "Parentesco com agressor",
    "O043": "Vitima denunciou",
    "O044": "Agressor foi responsabilizado",
    "O045": "Recebeu orientação legal",
    "O046": "Local de atendimento médico",
    "O047": "Recebeu atendimento médico imediato",
    "O048": "Recebeu atendimento posterior"
}

mapeamentos = {
    "V0001": {
            "11": "Rondônia",
            "12": "Acre",
            "13": "Amazonas",
            "14": "Roraima",
            "15": "Pará",
            "16": "Amapá",
            "17": "Tocantins",
            "21": "Maranhão",
            "22": "Piauí",
            "23": "Ceará",
            "24": "Rio Grande do Norte",
            "25": "Paraíba",
            "26": "Pernambuco",
            "27": "Alagoas",
            "28": "Sergipe",
            "29": "Bahia",
            "31": "Minas Gerais",
            "32": "Espírito Santo",
            "33": "Rio de Janeiro",
            "35": "São Paulo",
            "41": "Paraná",
            "42": "Santa Catarina",
            "43": "Rio Grande do Sul",
            "50": "Mato Grosso do Sul",
            "51": "Mato Grosso",
            "52": "Goiás",
            "53": "Distrito Federal",
            "": "Não informado"  
        },
    "C006": {"1": "Masculino", "2": "Feminino"},
    "C008": {str(i).zfill(3): str(i) for i in range(0, 131)},
    "C009": {"1": "Branca", "2": "Preta", "3": "Amarela", "4": "Parda", "5": "Indígena", "9": "Ignorado"},
    "V0026": {"1": "Urbano", "2": "Rural", "": "Não aplicável"},
    "O025": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O027": {"1": "Física", "2": "Sexual", "3": "Psicológica", "4": "Outro", "": "Não aplicável"},
    "O028": {
        "1": "Com arma de fogo (revólver, escopeta, pistola)",
        "2": "Com objeto pérfuro-cortante (faca, navalha, punhal, tesoura)",
        "3": "Com objeto contundente (pau, cassetete, barra de ferro, pedra)",
        "4": "Com força corporal, espancamento (tapa, murro, empurrão)",
        "5": "Por meio de palavras ofensivas, xingamentos ou palavrões",
        "6": "Outro",
        "": "Não aplicável"
    },
    "O029": {
        "1": "Residência",
        "2": "Trabalho",
        "3": "Escola/Faculdade ou similar",
        "4": "Bar ou similar",
        "5": "Via pública",
        "6": "Banco/Caixa eletrônico/Lotérica",
        "7": "Outro",
        "": "Não aplicável"
    },
    "O030": {"1": "Bandido, ladrão ou assaltante", "2": "Agente legal público", "3": "Outro", "": "Não aplicável"},
    "O031": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O032": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O033": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O034": {
        "01": "No local da violência",
        "02": "Unidade básica de saúde",
        "03": "Centro de Especialidades/Policlínica/PAM",
        "04": "UPA",
        "05": "Outro tipo de Pronto Atendimento Público",
        "06": "Pronto-socorro ou emergência de hospital público",
        "07": "Hospital público/ambulatório",
        "08": "Consultório particular ou clínica privada",
        "09": "Ambulatório ou consultório de empresa ou sindicato",
        "10": "Pronto-atendimento ou emergência de hospital privado",
        "11": "No domicílio, com médico particular",
        "12": "No domicílio, com médico da equipe de saúde da família",
        "13": "Outro",
        "": "Não aplicável"
    },
    "O035": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O036": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O037": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O038": {"1": "Uma vez", "2": "Duas vezes", "3": "De três a seis vezes", "4": "De sete a menos de 12 vezes",
             "5": "Pelo menos uma vez por mês", "6": "Pelo menos uma vez por semana", "7": "Quase diariamente", "": "Não aplicável"},
    "O039": {"1": "Física", "2": "Sexual", "3": "Psicológica", "4": "Outra", "": "Não aplicável"},
    "O040": {"1": "Com força corporal", "2": "Com arma de fogo", "3": "Com objeto pérfuro-cortante", "4": "Com objeto contundente",
             "5": "Com arremesso de substância/objeto quente", "6": "Com lançamento de objetos", "7": "Com envenenamento", "8": "Por meio de palavras ofensivas", "9": "Outro", "": "Não aplicável"},
    "O041": {"1": "Residência", "2": "Trabalho", "3": "Escola / Faculdade ou similar", "4": "Bar ou similar", "5": "Via pública", "6": "Outro", "": "Não aplicável"},
    "O042": {"01": "Cônjuge/companheiro(a)/namorado(a)", "02": "Ex-cônjuge/ex-companheiro(a)/ex-namorado(a)", "03": "Pai/Mãe", "04": "Padrasto/Madrasta",
             "05": "Filho(a)", "06": "Irmão(ã)", "07": "Outro parente", "08": "Amigos(as)/colegas", "09": "Patrão/chefe", "10": "Outra pessoa conhecida", "": "Não aplicável"},
    "O043": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O044": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O045": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O046": {
        "01": "No local da agressão", "02": "Unidade básica de saúde", "03": "Centro de Especialidades/Policlínica/PAM", "04": "UPA",
        "05": "Outro tipo de Pronto Atendimento Público", "06": "Pronto-socorro ou emergência de hospital público",
        "07": "Hospital público/ambulatório", "08": "Consultório particular ou clínica privada", "09": "Ambulatório ou consultório de empresa ou sindicato",
        "10": "Pronto-atendimento ou emergência de hospital privado", "11": "No domicílio, com médico particular", "12": "No domicílio, com médico da equipe de saúde da família",
        "13": "Outro", "": "Não aplicável"
    },
    "O047": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "O048": {"1": "Sim", "2": "Não", "": "Não aplicável"}
}

def extrair_linha(linha):
    resultado = {}
    for col, (inicio, fim) in colunas_posicoes.items():
        valor = linha[inicio:fim].strip()
        resultado[col] = mapeamentos.get(col, {}).get(valor, valor)
    return resultado

dados = []
with open(microdados_dir, "PNS_2013.txt", "r", encoding="utf-8") as f:
    for linha in f:
        if linha.strip():
            dados.append(extrair_linha(linha))

df = pd.DataFrame(dados)

df.rename(columns={col: nome for col, nome in colunas_nomes.items()}, inplace=True)

df = df[(df["Vítima sofreu violência"] == "Sim") & (df["Sexo"] == "Feminino")]

df.to_csv(processed_dir, "pns_violencia_2013.csv", index=False, sep=";", encoding="utf-8-sig")
print("\nArquivo salvo com sucesso em:", processed_dir)
