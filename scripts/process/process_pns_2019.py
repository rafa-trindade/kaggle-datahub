import pandas as pd

base_dir = "/opt/airflow" 
data_dir = os.path.join(base_dir, "data")
microdados_dir = os.path.join(base_dir, "utils", "microdados_ibge")
processed_dir = os.path.join(data_dir, "processed")
os.makedirs(processed_dir, exist_ok=True)

colunas_posicoes = {
    "V0001": (0, 2),
    "V0026": (30, 31),        
    "C006":  (107, 108),      
    "C008":  (116, 119),      
    "C009":  (119, 120),      
    "V00201": (1247, 1248), 
    "V00202": (1248, 1249),   
    "V00203": (1249, 1250), 
    "V00204": (1250, 1251),  
    "V00205": (1251, 1252),  
    "V003":   (1252, 1253),  
    "V006":   (1253, 1255),  
    "V007":   (1255, 1256),  
    "V01401": (1256, 1257),  
    "V01402": (1257, 1258),  
    "V01403": (1258, 1259),  
    "V01404": (1259, 1260),  
    "V01405": (1260, 1261),  
    "V015":   (1261, 1262),  
    "V018":   (1262, 1264),  
    "V019":   (1264, 1265),  
    "V02701": (1265, 1266),  
    "V02702": (1266, 1267),   
    "V02801": (1267, 1268),  
    "V02802": (1268, 1269),   
    "V029":   (1269, 1270), 
    "V032":   (1270, 1272), 
    "V033":   (1272, 1273),  
    "V034":   (1273, 1274), 
    "V03501": (1274, 1275), 
    "V03502": (1275, 1276), 
    "V03503": (1276, 1277), 
    "V036":   (1277, 1278), 
    "V037":   (1278, 1279), 
    "V038":   (1279, 1281), 
    "V039":   (1281, 1282)
}

colunas_nomes = {
    "V0001": "Unidade da Federação",
    "V0026": "Área",
    "C006": "Sexo",
    "C008": "Idade",
    "C009": "Cor ou raça",
    "V00201": "Ofensa/humilhação em público (12 meses)",
    "V00202": "Gritos/xingamentos (12 meses)",
    "V00203": "Ameaças/redes sociais (12 meses)",
    "V00204": "Ameaça contra pessoa importante (12 meses)",
    "V00205": "Destruiu algo seu (12 meses)",
    "V003":   "Frequência da violência psicológica",
    "V006":   "Autor da violência psicológica",
    "V007":   "Local da violência psicológica",
    "V01401": "Agressão física: tapa/bofetada",
    "V01402": "Agressão física: empurrão/segurar/jogar objeto",
    "V01403": "Agressão física: soco/chute/puxão de cabelo",
    "V01404": "Agressão física: estrangular/asfixiar/queimar",
    "V01405": "Agressão física com arma (faca, arma de fogo)",
    "V015":   "Frequência da agressão física",
    "V018": "Autor da agressão física",
    "V019": "Local da agressão física",
    "V02701": "Violência sexual (12 meses): toque/beijo/manipulação",
    "V02702": "Violência sexual (12 meses): ameaça/forçar ato sexual",
    "V02801": "Violência sexual na vida: toque/beijo/manipulação",
    "V02802": "Violência sexual na vida: ameaça/forçar ato sexual",
    "V029":  "Frequência da violência sexual",
    "V032":  "Autor da violência sexual",
    "V033":  "Local da violência sexual",
    "V034":  "Deixou de realizar atividades por causa da violência sexual",
    "V03501": "Consequência física por ato sexual forçado",
    "V03502": "Consequência psicológica por ato sexual forçado",
    "V03503": "DST ou gravidez por ato sexual forçado",
    "V036": "Buscou atendimento de saúde",
    "V037": "Recebeu atendimento de saúde",
    "V038": "Local do atendimento de saúde",
    "V039": "Internação por 24h ou mais",
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
        "": "Não aplicável"
    },
    "V0026": {"1": "Urbano", "2": "Rural", "": "Não aplicável"},
    "C006": {
        "1": "Masculino",
        "2": "Feminino",
        "": "Não aplicável"
    },
    "C008": {
        "": "Não aplicável"
    },
    "C009": {
        "1": "Branca",
        "2": "Preta",
        "3": "Amarela",
        "4": "Parda",
        "5": "Indígena",
        "9": "Ignorado",
        "": "Não aplicável"
    },
    "V00201": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V00202": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V00203": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V00204": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V00205": {"1": "Sim", "2": "Não", "": "Não aplicável"},

    "V003": {
        "1": "Muitas vezes",
        "2": "Algumas vezes",
        "3": "Uma vez",
        "": "Não aplicável"
    },
    "V006": {
        "01": "Cônjuge/companheiro(a)",
        "02": "Ex-cônjuge/companheiro(a)",
        "03": "Namorado(a)/ex",
        "04": "Pai/mãe/padrasto/madrasta",
        "05": "Filho(a)/enteado(a)",
        "06": "Irmão(ã)",
        "07": "Outro parente",
        "08": "Amigo/colega/vizinho",
        "09": "Empregado(a)",
        "10": "Patrão/chefe",
        "11": "Pessoa desconhecida",
        "12": "Policial",
        "13": "Outro",
        "": "Não aplicável"
    },
    "V007": {
        "1": "Residência",
        "2": "Trabalho",
        "3": "Estudo",
        "4": "Bar/restaurante",
        "5": "Via pública",
        "6": "Internet/Redes sociais",
        "7": "Outro",
        "": "Não aplicável"
    },
    "V01401": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V01402": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V01403": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V01404": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V01405": {"1": "Sim", "2": "Não", "": "Não aplicável"},

    "V015": {"1": "Muitas vezes", "2": "Algumas vezes", "3": "Uma vez", "": "Não aplicável"},

    "V018": { 
        "01": "Cônjuge/companheiro(a)",
        "02": "Ex-cônjuge/companheiro(a)",
        "03": "Namorado(a)/ex",
        "04": "Pai/mãe/padrasto/madrasta",
        "05": "Filho(a)/enteado(a)",
        "06": "Irmão(ã)",
        "07": "Outro parente",
        "08": "Amigo/colega/vizinho",
        "09": "Empregado(a)",
        "10": "Patrão/chefe",
        "11": "Pessoa desconhecida",
        "12": "Policial",
        "13": "Outro",
        "": "Não aplicável"
    },

    "V019": {
        "1": "Residência",
        "2": "Trabalho",
        "3": "Estudo",
        "4": "Bar/restaurante",
        "5": "Via pública",
        "6": "Outro",
        "": "Não aplicável"
    },
    "V02701": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V02702": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V02801": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V02802": {"1": "Sim", "2": "Não", "": "Não aplicável"},

    "V029": {"1": "Muitas vezes", "2": "Algumas vezes", "3": "Uma vez", "": "Não aplicável"},

    "V032": {  
        "01": "Cônjuge/companheiro(a)",
        "02": "Ex-cônjuge/companheiro(a)",
        "03": "Namorado(a)/ex",
        "04": "Pai/mãe/padrasto/madrasta",
        "05": "Filho(a)/enteado(a)",
        "06": "Irmão(ã)",
        "07": "Outro parente",
        "08": "Amigo/colega/vizinho",
        "09": "Empregado(a)",
        "10": "Patrão/chefe",
        "11": "Pessoa desconhecida",
        "12": "Policial",
        "13": "Outro",
        "": "Não aplicável"
    },
    "V033": {
        "1": "Residência",
        "2": "Trabalho",
        "3": "Estudo",
        "4": "Bar/restaurante",
        "5": "Via pública",
        "6": "Outro",
        "": "Não aplicável"
    },
    "V034": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V03501": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V03502": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V03503": {"1": "Sim", "2": "Não", "": "Não aplicável"},
    "V036": {"1": "Sim", "2": "Não", "" : "Não aplicável"},
    "V037": {"1": "Sim", "2": "Não", "" : "Não aplicável"},
    "V038": {
        "01": "No local",
        "02": "Farmácia",
        "03": "UBS/PSF",
        "04": "Policlínica",
        "05": "UPA/pronto atendimento público",
        "06": "Ambulatório hospital público",
        "07": "Consultório/Clínica privada",
        "08": "Pronto atendimento privado",
        "09": "Atendimento domiciliar",
        "10": "Outro serviço",
        "": "Não aplicável"
    },

    "V039": {"1": "Sim", "2": "Não", "": "Não aplicável"},
}

def extrair_linha(linha):
    resultado = {}
    for col, (inicio, fim) in colunas_posicoes.items():
        valor = linha[inicio:fim].rstrip()
        resultado[col] = mapeamentos.get(col, {}).get(valor, valor)
    return resultado

dados = []
with open(microdados_dir, "PNS_2019.txt", "r", encoding="utf-8") as f:
    for linha in f:
        if linha.strip():
            dados.append(extrair_linha(linha))

df = pd.DataFrame(dados)

df.rename(columns={col: nome for col, nome in colunas_nomes.items()}, inplace=True)

df["Idade"] = df["Idade"].str.lstrip("0")

df = df[df["Sexo"] == "Feminino"]
violencias = [
    "Ofensa/humilhação em público (12 meses)",
    "Gritos/xingamentos (12 meses)",
    "Ameaças/redes sociais (12 meses)",
    "Ameaça contra pessoa importante (12 meses)",
    "Destruiu algo seu (12 meses)",
    "Agressão física: tapa/bofetada",
    "Agressão física: empurrão/segurar/jogar objeto",
    "Agressão física: soco/chute/puxão de cabelo",
    "Agressão física: estrangular/asfixiar/queimar",
    "Agressão física com arma (faca, arma de fogo)",
    "Violência sexual (12 meses): toque/beijo/manipulação",
    "Violência sexual (12 meses): ameaça/forçar ato sexual",
]
df = df[df[violencias].eq("Sim").any(axis=1)]

df.to_csv(processed_dir, "pns_violencia_2019.csv", index=False, sep=";", encoding="utf-8-sig")
print("\nArquivo salvo com sucesso em:", processed_dir)
