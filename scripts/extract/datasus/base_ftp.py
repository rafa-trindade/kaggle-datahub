import os
import socket
import time
from ftplib import FTP, error_perm
from typing import Callable, List

### ssh -R 1080 -N rafael@IP_VPS
### pip install PySocks
import socks
socks.set_default_proxy(socks.SOCKS5, "127.0.0.1", 1080)
socket.socket = socks.socksocket
print("Roteamento Reverso Ativado! Saindo pela internet do PC local...")

FTP_HOST = "ftp.datasus.gov.br"
MAX_RETRIES = 10  
RETRY_DELAY = 5   

def ensure_output_dir(path: str):
    os.makedirs(path, exist_ok=True)

def get_tamanho_ftp(ftp: FTP, nome_arquivo: str) -> int | None:
    try:
        return ftp.size(nome_arquivo)
    except error_perm:
        return None

def baixar_arquivo(ftp_dir: str, nome_arquivo: str, pasta_saida: str) -> bool:
    local_path = os.path.join(pasta_saida, nome_arquivo)
    
    for attempt in range(MAX_RETRIES):
        try:
            ip_v4 = socket.gethostbyname(FTP_HOST)
            
            with FTP() as ftp:
                ftp.connect(ip_v4, 21, timeout=30)
                ftp.login()
                ftp.set_pasv(True)
                ftp.cwd(ftp_dir)
                
                tamanho_ftp = get_tamanho_ftp(ftp, nome_arquivo)
                if not tamanho_ftp:
                    print(f"[ERRO] Não foi possível obter tamanho de {nome_arquivo}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue
                    return False
                
                tamanho_local = os.path.getsize(local_path) if os.path.exists(local_path) else 0
                
                if tamanho_local >= tamanho_ftp:
                    print(f"[SKIP] {nome_arquivo} (Completo: {tamanho_local} bytes)")
                    return True 
                
                rest_pos = tamanho_local if tamanho_local > 0 else None
                modo_abertura = "ab" if tamanho_local > 0 else "wb"
                
                if rest_pos:
                    print(f"[RESUME] {nome_arquivo} do byte {rest_pos} (Tentativa {attempt + 1}/{MAX_RETRIES})")
                else:
                    print(f"[DOWN] {nome_arquivo} (Tentativa {attempt + 1}/{MAX_RETRIES})")

                with open(local_path, modo_abertura) as f:
                    ftp.sock.settimeout(300)
                    ftp.retrbinary(f"RETR {nome_arquivo}", f.write, rest=rest_pos, blocksize=32768)
                
                if os.path.getsize(local_path) == tamanho_ftp:
                    print(f"[OK] {nome_arquivo} concluído.")
                    return True
                else:
                    raise Exception("Download interrompido (tamanho incompleto)")
                
        except (socket.timeout, EOFError, ConnectionResetError, Exception) as e:
            print(f"[ERRO] Falha em {nome_arquivo} (Tentativa {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Aguardando {RETRY_DELAY}s para reconectar e tentar novamente...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"[FATAL] Desistindo de {nome_arquivo} após {MAX_RETRIES} tentativas.")
                if os.path.exists(local_path) and os.path.getsize(local_path) < (tamanho_ftp or 0):
                    os.remove(local_path)
                return False

def sincronizar_ftp(ftp_dir: str, output_dir: str, regra_filtro: Callable[[str], bool]) -> bool:
    ensure_output_dir(output_dir)
    print(f"Conectando a {FTP_HOST} ({ftp_dir}) para listar arquivos...")
    arquivos_baixados = False
    relevantes = []

    for attempt in range(MAX_RETRIES):
        try:
            ip_v4 = socket.gethostbyname(FTP_HOST)
            with FTP() as ftp:
                ftp.connect(ip_v4, 21, timeout=30)
                ftp.login()
                ftp.set_pasv(True)
                ftp.cwd(ftp_dir)
                
                ftp.sock.settimeout(60)
                arquivos = ftp.nlst()
                
                if not arquivos:
                    print("Nenhum arquivo encontrado no diretório.")
                    return False
                    
                relevantes = [arq for arq in arquivos if regra_filtro(arq)]
                print(f"Sucesso ao listar! {len(relevantes)} arquivos passaram no filtro.")
                break 
                
        except Exception as e:
            print(f"[ERRO] Falha ao listar diretório (Tentativa {attempt + 1}): {e}")
            if attempt == MAX_RETRIES - 1:
                print("[FATAL] Não foi possível listar os arquivos do FTP.")
                return False
            time.sleep(RETRY_DELAY)

    for arq in relevantes:
        if baixar_arquivo(ftp_dir, arq, output_dir):
            arquivos_baixados = True

    if arquivos_baixados:
        print("[INFO] Sincronização concluída com novos arquivos.")
    else:
        print("[INFO] Sincronização concluída. Nenhuma atualização necessária.")
        
    return arquivos_baixados