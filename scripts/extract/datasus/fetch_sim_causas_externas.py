import os
from ftplib import FTP, error_perm
from pathlib import Path
from typing import List

# -----------------------------
# Configurações
# -----------------------------
FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR = "/dissemin/publicos/SIM/CID10/DOFET"
OUTPUT_DIR = "data/utils/dbc_sim_causas_externas"

ANO_MINIMO = 2006
ANO_INICIO_DIGITOS = 6

import datetime
ANO_ATUAL_DIGITOS = datetime.date.today().year % 100 

# -----------------------------
# Funções Auxiliares (Inalteradas)
# -----------------------------
def ensure_output_dir(path: str):
    """Cria o diretório de saída se não existir."""
    os.makedirs(path, exist_ok=True)

def listar_arquivos_dbc(ftp: FTP) -> List[str]:
    """Lista todos os arquivos .dbc no diretório FTP."""
    arquivos = []
    def coletor(nome):
        if nome.lower().endswith(".dbc"):
            arquivos.append(nome)
    try:
        ftp.cwd(FTP_DIR)
        ftp.retrlines("NLST", coletor)
    except error_perm as e:
        print(f"[ERRO] Falha ao acessar o diretório FTP {FTP_DIR}: {e}")
        return []
    return arquivos

def get_tamanho_ftp(ftp: FTP, nome_arquivo: str) -> int | None:
    """Obtém o tamanho do arquivo no FTP."""
    try:
        return ftp.size(nome_arquivo)
    except error_perm:
        return None

def precisa_baixar(ftp: FTP, nome_arquivo: str, pasta_saida: str) -> bool:
    """Verifica se o arquivo precisa ser baixado (não existe ou tamanho diferente)."""
    local_path = os.path.join(pasta_saida, nome_arquivo)
    if not os.path.exists(local_path):
        return True
    tamanho_local = os.path.getsize(local_path)
    tamanho_ftp = get_tamanho_ftp(ftp, nome_arquivo)
    if tamanho_ftp is None:
        print(f"[INFO] Não foi possível obter SIZE de {nome_arquivo} no FTP. Pulando verificação de tamanho.")
        return False
    if tamanho_local == tamanho_ftp:
        print(f"[SKIP] {nome_arquivo} (mesmo tamanho: {tamanho_local})")
        return False
    else:
        print(f"[WARN] {nome_arquivo} existe, mas tamanho diferente "
              f"(local: {tamanho_local}, FTP: {tamanho_ftp}) -> rebaixando.")
        return True

def baixar_arquivo(ftp: FTP, nome_arquivo: str, pasta_saida: str) -> bool:
    """Baixa um arquivo do FTP se necessário."""
    local_path = os.path.join(pasta_saida, nome_arquivo)
    if not precisa_baixar(ftp, nome_arquivo, pasta_saida):
        return False
    print(f"[DOWN] {nome_arquivo}")
    try:
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {nome_arquivo}", f.write)
        print(f"[OK] {local_path}")
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao baixar {nome_arquivo}: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return False

# -----------------------------
# Função Principal
# -----------------------------

def main() -> bool:
    ensure_output_dir(OUTPUT_DIR)

    print(f"Conectando a {FTP_HOST}...")
    arquivos_baixados = False

    try:
        with FTP(FTP_HOST, timeout=60) as ftp:
            ftp.login() 
            arquivos = listar_arquivos_dbc(ftp)

            if not arquivos:
                print("Nenhum .dbc encontrado ou falha de conexão.")
                return False

            relevantes = []
            ano_inicio = ANO_INICIO_DIGITOS
            ano_fim = ANO_ATUAL_DIGITOS

            for arq in arquivos:
                if arq.upper().startswith("DOEXT") and arq.upper().endswith(".DBC"):
                    
                    try:
                        ano_str = arq[5:7] 
                        
                        if ano_str.isdigit():
                            ano_file = int(ano_str)
                            
                            if ano_file >= ano_inicio and ano_file <= ano_fim:
                                relevantes.append(arq)

                    except (ValueError, IndexError):
                        continue

            print(
                f"Encontrados {len(arquivos)} arquivos .dbc. "
                f"Filtrando {len(relevantes)} arquivos relevantes (DOEXT, de {ANO_MINIMO} em diante)."
            )

            for arq in relevantes:
                if baixar_arquivo(ftp, arq, OUTPUT_DIR):
                    arquivos_baixados = True

    except Exception as e:
        print(f"[ERRO CRÍTICO] Falha na conexão ou operação principal: {e}")
        return False

    print("[INFO] Arquivos novos/atualizados." if arquivos_baixados else "[INFO] Nenhuma atualização.")
    return arquivos_baixados


if __name__ == "__main__":
    updated = main()
    exit(0 if updated else 1)