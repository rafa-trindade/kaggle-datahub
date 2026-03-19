import requests
import zipfile
from io import BytesIO
from pathlib import Path

# -----------------------------------------
# Gerenciamento Central de Caminhos
# -----------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
LANDING_DIR = BASE_DIR / "data" / "landing"

RAW_DIR.mkdir(parents=True, exist_ok=True)
LANDING_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------------------
# Funções Utilitárias Reutilizáveis
# -----------------------------------------
def baixar_e_extrair_csv(url: str, caminho_destino: Path):
    """
    Baixa um arquivo ZIP da URL, extrai o primeiro arquivo .csv encontrado
    e salva no caminho de destino (Landing Zone).
    """
    print(f"Baixando: {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    print("Descompactando o arquivo ZIP...")
    with zipfile.ZipFile(BytesIO(response.content)) as z:
        csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
        
        with z.open(csv_name) as csvfile:
            with open(caminho_destino, "wb") as f_out:
                f_out.write(csvfile.read())
                
    print(f"✔ Arquivo salvo em Landing: {caminho_destino.name}")