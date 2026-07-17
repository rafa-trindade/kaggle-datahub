import os
import socket
import time
import random
import logging
from ftplib import FTP, error_perm
from typing import Callable, List

from scripts.common.bucket_sync import already_in_bucket, carregar_manifesto

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("datasus_ftp")

FTP_HOST = "ftp.datasus.gov.br"
MAX_RETRIES = 10
RETRY_DELAY = 5

# -----------------------------------------------------------------------
# Proxy reverso SOCKS5 (opcional) -- para contornar bloqueio de porta 21
# em VPS/cloud. Antes deste patch, isso era ativado incondicionalmente
# na importação do módulo (efeito colateral perigoso: qualquer script que
# importasse base_ftp.py, mesmo sem precisar de FTP, tinha TODOS os
# sockets do processo redirecionados para o proxy). Agora é opt-in.
#
# Para ativar: configure as variáveis de ambiente abaixo e rode
#   ssh -R 1080 -N usuario@IP_VPS
# antes de executar os scripts de extract do DATASUS.
# -----------------------------------------------------------------------
SOCKS5_PROXY_ENABLED = os.environ.get("SOCKS5_PROXY_ENABLED", "false").lower() in ("1", "true", "yes")
SOCKS5_PROXY_HOST = os.environ.get("SOCKS5_PROXY_HOST", "127.0.0.1")
SOCKS5_PROXY_PORT = int(os.environ.get("SOCKS5_PROXY_PORT", "1080"))

if SOCKS5_PROXY_ENABLED:
    import socks  # PySocks -- só é importado se o proxy estiver ativado
    socks.set_default_proxy(socks.SOCKS5, SOCKS5_PROXY_HOST, SOCKS5_PROXY_PORT)
    socket.socket = socks.socksocket
    logger.info(f"[SOCKS5] Roteamento reverso ativado ({SOCKS5_PROXY_HOST}:{SOCKS5_PROXY_PORT}).")


class FTPPasvFix(FTP):
    """
    FTP com correção de PASV: ignora o IP devolvido pelo servidor na
    resposta 227 (comum estar "errado"/interno quando o servidor está
    atrás de load balancer/NAT) e conecta sempre no mesmo host já usado
    na conexão de controle, usando só a porta sugerida. Esse é o fix
    clássico para "funciona em rede doméstica, falha intermitente em
    VPS/cloud" com ftplib.
    """
    def makepasv(self):
        host, port = super().makepasv()
        host_real = self.sock.getpeername()[0]
        if host != host_real:
            logger.warning(f"[PASV] Servidor devolveu IP {host}, usando {host_real} (conexão de controle) em vez disso.")
        return host_real, port


def ensure_output_dir(path: str):
    os.makedirs(path, exist_ok=True)

def get_tamanho_ftp(ftp: FTP, nome_arquivo: str) -> int | None:
    try:
        return ftp.size(nome_arquivo)
    except error_perm:
        return None

def _backoff(attempt: int):
    """Backoff exponencial com jitter, em vez de delay fixo -- evita que
    retries fiquem sincronizados/martelando o servidor no mesmo instante
    se houver throttling do lado do DATASUS."""
    espera = min(RETRY_DELAY * (2 ** attempt), 120) + random.uniform(0, 3)
    logger.info(f"Aguardando {espera:.1f}s antes de tentar de novo...")
    time.sleep(espera)

def baixar_arquivo(ftp_dir: str, nome_arquivo: str, pasta_saida: str,
                    manifesto: dict[str, int] | None = None) -> tuple[bool, bool]:
    """Retorna (sucesso, houve_novidade). houve_novidade é False quando o
    arquivo já estava completo localmente OU já consta no manifesto com
    o mesmo tamanho (já foi incorporado ao último output publicado) --
    só True quando algo foi de fato baixado/retomado agora.

    manifesto: {nome_arquivo: tamanho} de arquivos-fonte já incorporados
    ao último output consolidado publicado (ver
    scripts.common.bucket_sync.carregar_manifesto) -- evita re-baixar e
    reprocessar um arquivo que não mudou desde a última execução."""
    local_path = os.path.join(pasta_saida, nome_arquivo)
    tamanho_ftp = None

    for attempt in range(MAX_RETRIES):
        try:
            ip_v4 = socket.gethostbyname(FTP_HOST)
            logger.info(f"[{nome_arquivo}] Tentativa {attempt + 1}/{MAX_RETRIES} -- conectando em {ip_v4}")

            with FTPPasvFix() as ftp:
                ftp.connect(ip_v4, 21, timeout=30)
                ftp.login()
                ftp.set_pasv(True)
                ftp.cwd(ftp_dir)

                tamanho_ftp = get_tamanho_ftp(ftp, nome_arquivo)
                if not tamanho_ftp:
                    print(f"[ERRO] Não foi possível obter tamanho de {nome_arquivo}")
                    if attempt < MAX_RETRIES - 1:
                        _backoff(attempt)
                        continue
                    return False, False

                if manifesto is not None and manifesto.get(nome_arquivo) == tamanho_ftp:
                    print(f"[SKIP-MANIFESTO] {nome_arquivo} já incorporado ao último output publicado.")
                    return True, False

                tamanho_local = os.path.getsize(local_path) if os.path.exists(local_path) else 0

                if tamanho_local >= tamanho_ftp:
                    print(f"[SKIP] {nome_arquivo} (Completo: {tamanho_local} bytes)")
                    return True, False

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
                    return True, True
                else:
                    raise Exception("Download interrompido (tamanho incompleto)")

        except (socket.timeout, EOFError, ConnectionResetError, Exception) as e:
            logger.error(f"[{nome_arquivo}] Falha na tentativa {attempt + 1}: {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES - 1:
                _backoff(attempt)
            else:
                print(f"[FATAL] Desistindo de {nome_arquivo} após {MAX_RETRIES} tentativas.")
                if os.path.exists(local_path) and os.path.getsize(local_path) < (tamanho_ftp or 0):
                    os.remove(local_path)
                return False, False
    return False, False

def sincronizar_ftp(ftp_dir: str, output_dir: str, regra_filtro: Callable[[str], bool],
                     pasta_bucket: str | None = None) -> tuple[bool, bool]:
    """Retorna (sucesso, houve_novidade).

    pasta_bucket: se informada, carrega o manifesto dessa pasta UMA VEZ
    (não um head_object por arquivo) e usa pra decidir quais arquivos
    pular -- ver scripts.common.bucket_sync.carregar_manifesto. Se não
    informada, a checagem contra o bucket é pulada (só compara contra
    disco local)."""
    ensure_output_dir(output_dir)
    logger.info(f"Conectando a {FTP_HOST} ({ftp_dir}) para listar arquivos...")
    relevantes = []

    manifesto = carregar_manifesto(pasta_bucket) if pasta_bucket else None

    for attempt in range(MAX_RETRIES):
        try:
            ip_v4 = socket.gethostbyname(FTP_HOST)
            with FTPPasvFix() as ftp:
                ftp.connect(ip_v4, 21, timeout=30)
                ftp.login()
                ftp.set_pasv(True)
                ftp.cwd(ftp_dir)

                ftp.sock.settimeout(60)
                arquivos = ftp.nlst()

                if not arquivos:
                    print("Nenhum arquivo encontrado no diretório.")
                    return True, False  # diretório vazio não é erro, só não tem novidade

                relevantes = [arq for arq in arquivos if regra_filtro(arq)]
                print(f"Sucesso ao listar! {len(relevantes)} arquivos passaram no filtro.")
                break

        except Exception as e:
            logger.error(f"Falha ao listar diretório (Tentativa {attempt + 1}): {type(e).__name__}: {e}")
            if attempt == MAX_RETRIES - 1:
                print("[FATAL] Não foi possível listar os arquivos do FTP.")
                return False, False
            _backoff(attempt)

    sucesso_geral = True
    houve_novidade = False
    for arq in relevantes:
        sucesso, novidade = baixar_arquivo(ftp_dir, arq, output_dir, manifesto=manifesto)
        sucesso_geral = sucesso_geral and sucesso
        houve_novidade = houve_novidade or novidade

    if houve_novidade:
        print("[INFO] Sincronização concluída com novos arquivos.")
    else:
        print("[INFO] Sincronização concluída. Nenhuma atualização necessária.")

    return sucesso_geral, houve_novidade