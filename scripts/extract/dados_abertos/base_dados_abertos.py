import requests
import zipfile
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta

from scripts.common.paths import BASE_DIR, RAW_DIR, LANDING_DIR  # noqa: F401

# -----------------------------------------
# Funções Utilitárias Reutilizáveis
# -----------------------------------------
def verificar_novidade_http(url: str, pasta_bucket: str, chave_manifesto: str) -> tuple[bool, int]:
    """Retorna (houve_novidade, tamanho_remoto). Compara o Content-Length
    da URL contra o manifesto no bucket -- mesma ideia do manifesto usado
    nas fontes FTP (scripts.common.bucket_sync), adaptado pra fontes HTTP
    de arquivo único (sem listagem de diretório pra comparar item a
    item). Se o servidor não informar Content-Length, trata como
    novidade sempre (mais seguro que arriscar pular um dado que mudou)."""
    from scripts.common.bucket_sync import carregar_manifesto

    resposta = requests.head(url, timeout=30, allow_redirects=True)
    tamanho_remoto = int(resposta.headers.get("Content-Length", 0))

    if tamanho_remoto == 0:
        return True, tamanho_remoto

    manifesto = carregar_manifesto(pasta_bucket)
    tamanho_anterior = manifesto.get(chave_manifesto)
    return tamanho_anterior != tamanho_remoto, tamanho_remoto


def tentar_datas_e_baixar(slug: str, dias_tentativa: int = 15) -> tuple[bytes | None, str | None]:
    """Tenta os últimos `dias_tentativa` dias pra trás até achar um ZIP
    válido pra esse slug (ex: 'convenios', 'licitacoes', 'despesas') no
    mecanismo de Dados Abertos do Portal da Transparência. Retorna
    (conteúdo_do_zip, data_encontrada) ou (None, None) se não achar
    nenhum nos dias tentados."""
    hoje = datetime.now()
    for i in range(dias_tentativa):
        data = hoje - timedelta(days=i)
        data_str = data.strftime("%Y%m%d")
        url = f"https://portaldatransparencia.gov.br/download-de-dados/{slug}/{data_str}"
        print(f"Tentando {data_str}...")
        try:
            resposta = requests.head(url, timeout=15, allow_redirects=True)
        except requests.RequestException as e:
            print(f"  [SKIP] {data_str} -- erro de conexão: {e}")
            continue

        content_type = resposta.headers.get("Content-Type", "").lower()
        if resposta.status_code == 200 and "zip" in content_type:
            tamanho = resposta.headers.get("Content-Length", "desconhecido")
            print(f"✔ Encontrado: {data_str} ({tamanho} bytes, Content-Type: {content_type})")
            resposta_get = requests.get(url, timeout=180)
            resposta_get.raise_for_status()
            return resposta_get.content, data_str
        print(f"  [SKIP] {data_str} não é um arquivo válido (status {resposta.status_code}).")

    return None, None


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