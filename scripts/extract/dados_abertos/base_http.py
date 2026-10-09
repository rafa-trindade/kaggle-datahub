"""Utilitários HTTP compartilhados por fontes que publicam arquivos soltos
via HTTP (índice de diretório Apache da ANS, buckets do OpenDataSUS etc).

Mesma filosofia do base_ftp.py: a detecção de novidade é feita por
tamanho (Content-Length) contra o manifesto do bucket, e o download é
resiliente (retry com backoff, arquivo .part renomeado só no final).
"""
import os
import re
import time
import random
import logging
from pathlib import Path
from urllib.parse import urljoin, unquote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("http_sync")

MAX_RETRIES_DOWNLOAD = 6
USER_AGENT = "kaggle-datahub/1.0 (+https://github.com/rafa-trindade/kaggle-datahub)"


def criar_sessao() -> requests.Session:
    """Session com retry automático para HEAD/GET (erros transitórios e 5xx)."""
    sessao = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5, backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["HEAD", "GET"]),
    )
    adaptador = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    sessao.mount("https://", adaptador)
    sessao.mount("http://", adaptador)
    # "identity": sem compressão no transporte. Alguns CDNs (CloudFront) variam
    # o Content-Length conforme comprimem ou não, o que faria o mesmo arquivo
    # parecer "novo" a cada execução.
    sessao.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "identity"})
    return sessao


def listar_indice_http(sessao: requests.Session, url_dir: str, padrao: re.Pattern) -> list[str]:
    """Lê um índice de diretório (Apache autoindex) e devolve os nomes de
    arquivo cujo href casa com `padrao`, deduplicados e ordenados."""
    if not url_dir.endswith("/"):
        url_dir += "/"
    resposta = sessao.get(url_dir, timeout=60)
    resposta.raise_for_status()
    nomes = set()
    for href in re.findall(r'href="([^"?#]+)"', resposta.text, re.IGNORECASE):
        nome = unquote(href.rsplit("/", 1)[-1])
        if padrao.match(nome):
            nomes.add(nome)
    return sorted(nomes)


def tamanho_remoto(sessao: requests.Session, url: str) -> int | None:
    """Content-Length via HEAD (None se o servidor não informar)."""
    resposta = sessao.head(url, timeout=60, allow_redirects=True)
    resposta.raise_for_status()
    tamanho = int(resposta.headers.get("Content-Length", 0) or 0)
    return tamanho or None


def _backoff(tentativa: int):
    espera = min(5 * (2 ** tentativa), 120) + random.uniform(0, 3)
    logger.info(f"Aguardando {espera:.1f}s antes de tentar de novo...")
    time.sleep(espera)


def baixar_http(sessao: requests.Session, url: str, destino: Path,
                tamanho_esperado: int | None = None) -> bool:
    """Baixa `url` para `destino` em streaming. Grava em .part e só renomeia
    no fim (arquivo final nunca fica pela metade). Se `destino` já existe
    com o tamanho esperado (execução anterior interrompida depois do
    download), não baixa de novo."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    if tamanho_esperado and destino.exists() and destino.stat().st_size == tamanho_esperado:
        print(f"[SKIP] {destino.name} já está na landing (completo).")
        return True

    parcial = destino.with_name(destino.name + ".part")
    for tentativa in range(MAX_RETRIES_DOWNLOAD):
        try:
            print(f"[DOWN] {url} (tentativa {tentativa + 1}/{MAX_RETRIES_DOWNLOAD})")
            with sessao.get(url, stream=True, timeout=(30, 300)) as resposta:
                resposta.raise_for_status()
                with open(parcial, "wb") as f:
                    for bloco in resposta.iter_content(chunk_size=1024 * 1024):
                        if bloco:
                            f.write(bloco)
            tamanho_baixado = parcial.stat().st_size
            if tamanho_esperado and tamanho_baixado != tamanho_esperado:
                raise IOError(f"tamanho incompleto ({tamanho_baixado} de {tamanho_esperado} bytes)")
            os.replace(parcial, destino)
            print(f"[OK] {destino.name} ({tamanho_baixado} bytes)")
            return True
        except Exception as e:
            logger.error(f"[{destino.name}] falha na tentativa {tentativa + 1}: {type(e).__name__}: {e}")
            parcial.unlink(missing_ok=True)
            if tentativa < MAX_RETRIES_DOWNLOAD - 1:
                _backoff(tentativa)
    print(f"[FATAL] Desistindo de {url} após {MAX_RETRIES_DOWNLOAD} tentativas.")
    return False


def url_arquivo(url_dir: str, nome: str) -> str:
    if not url_dir.endswith("/"):
        url_dir += "/"
    return urljoin(url_dir, nome)
