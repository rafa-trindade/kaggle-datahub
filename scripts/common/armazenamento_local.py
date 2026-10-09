"""Armazenamento local -- usado quando DATAHUB_STORAGE=local.

Em vez de um bucket MinIO, o "lake" passa a ser a própria pasta de
publicação do Kaggle (PUBLISH_CACHE_DIR), no mesmo layout que o loader já
usa:

    <PUBLISH_CACHE_DIR>/principal/<chave>             (ex.: principal/cnes/leitos.parquet)
    <PUBLISH_CACHE_DIR>/pa/<AAAA>/<arquivo>.parquet   (Produção Ambulatorial, por ano)
    <PUBLISH_CACHE_DIR>/pa/manifesto/_manifest.json
    <PUBLISH_CACHE_DIR>/pa/<demais arquivos de raiz do PA>

A classe ArmazenamentoLocal imita só o subconjunto da API do cliente boto3
que o projeto usa (head_bucket, head_object, get_object, put_object,
upload_file, download_file, get_paginator('list_objects_v2')), então o
restante do pipeline funciona sem saber onde os arquivos estão.

Toda gravação é atômica: escreve em '.<nome>.partial' na mesma pasta e só
então renomeia -- um processamento interrompido nunca deixa um parquet pela
metade no lugar do publicado.
"""
import io
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from scripts.common.paths import PUBLISH_CACHE_DIR

PREFIXO_PA = "producao_ambulatorial/"
DIR_PRINCIPAL = PUBLISH_CACHE_DIR / "principal"
DIR_PA = PUBLISH_CACHE_DIR / "pa"

# arquivos de controle do loader do Kaggle -- não são "objetos" do lake
ARQUIVOS_DE_CONTROLE = {"dataset-metadata.json", ".ultima_publicacao_sucesso"}
SUFIXO_PARCIAL = ".partial"


# ----------------------------------------------------------------------
# Mapeamento chave do lake <-> caminho local
# ----------------------------------------------------------------------
def caminho_local(chave: str) -> Path:
    """Chave no estilo bucket ('cnes/leitos.parquet') -> caminho na pasta do Kaggle."""
    chave = chave.lstrip("/")
    if chave.startswith(PREFIXO_PA):
        nome = chave.rsplit("/", 1)[-1]
        m = re.search(r"_(\d{4})\d{2}\.parquet$", nome)
        if m:
            return DIR_PA / m.group(1) / nome
        if nome == "_manifest.json":
            return DIR_PA / "manifesto" / nome
        return DIR_PA / nome
    return DIR_PRINCIPAL / Path(chave)


def _eh_arquivo_de_dados(p: Path, raiz: Path) -> bool:
    if not p.is_file():
        return False
    if p.name in ARQUIVOS_DE_CONTROLE or p.name.endswith(SUFIXO_PARCIAL):
        return False
    return True


def listar_chaves(prefixo: str = "") -> dict[str, Path]:
    """Todas as chaves do lake local (opcionalmente filtradas por prefixo) -> caminho."""
    resultado: dict[str, Path] = {}
    if DIR_PRINCIPAL.exists():
        for p in DIR_PRINCIPAL.rglob("*"):
            if _eh_arquivo_de_dados(p, DIR_PRINCIPAL):
                resultado[p.relative_to(DIR_PRINCIPAL).as_posix()] = p
    if DIR_PA.exists():
        for p in DIR_PA.rglob("*"):
            if _eh_arquivo_de_dados(p, DIR_PA):
                resultado[f"{PREFIXO_PA}{p.name}"] = p
    if prefixo:
        resultado = {k: v for k, v in resultado.items() if k.startswith(prefixo)}
    return dict(sorted(resultado.items()))


def limpar_parciais(raiz: Path) -> int:
    """Remove sobras '.<nome>.partial' de gravações interrompidas."""
    removidos = 0
    if raiz.exists():
        for p in raiz.rglob(f"*{SUFIXO_PARCIAL}"):
            try:
                p.unlink()
                removidos += 1
            except OSError:
                pass
    return removidos


def _destino_parcial(destino: Path) -> Path:
    return destino.with_name(f".{destino.name}{SUFIXO_PARCIAL}")


def gravar_atomico_movendo(origem: Path, chave: str) -> Path:
    """Move `origem` para o lugar da chave (rápido no mesmo disco; copia se for
    outro disco), substituindo o publicado só no fim."""
    destino = caminho_local(chave)
    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = _destino_parcial(destino)
    parcial.unlink(missing_ok=True)
    shutil.move(str(origem), str(parcial))
    os.replace(parcial, destino)
    os.utime(destino)  # "publicado agora" -- é o que o loader usa para detectar novidade
    return destino


def gravar_atomico_copiando(origem: Path, chave: str) -> Path:
    destino = caminho_local(chave)
    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = _destino_parcial(destino)
    parcial.unlink(missing_ok=True)
    shutil.copyfile(origem, parcial)
    os.replace(parcial, destino)
    os.utime(destino)
    return destino


# ----------------------------------------------------------------------
# Imitação mínima do cliente boto3
# ----------------------------------------------------------------------
def _erro_404(operacao: str, chave: str):
    from botocore.exceptions import ClientError
    return ClientError({"Error": {"Code": "404", "Message": f"{chave} não existe no lake local"}}, operacao)


class _Paginador:
    def paginate(self, Bucket=None, Prefix: str = "", **_):
        conteudo = []
        for chave, p in listar_chaves(Prefix).items():
            st = p.stat()
            conteudo.append({
                "Key": chave,
                "Size": st.st_size,
                "LastModified": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
            })
        yield {"Contents": conteudo}


class ArmazenamentoLocal:
    """Substituto do cliente S3 quando DATAHUB_STORAGE=local."""

    modo = "local"

    def head_bucket(self, Bucket=None, **_):
        PUBLISH_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        return {}

    def create_bucket(self, Bucket=None, **_):
        return self.head_bucket()

    def head_object(self, Bucket=None, Key: str = "", **_):
        p = caminho_local(Key)
        if not p.is_file():
            raise _erro_404("HeadObject", Key)
        return {"ContentLength": p.stat().st_size}

    def get_object(self, Bucket=None, Key: str = "", **_):
        p = caminho_local(Key)
        if not p.is_file():
            raise _erro_404("GetObject", Key)
        return {"Body": io.BytesIO(p.read_bytes())}

    def put_object(self, Bucket=None, Key: str = "", Body: bytes = b"", **_):
        destino = caminho_local(Key)
        destino.parent.mkdir(parents=True, exist_ok=True)
        parcial = _destino_parcial(destino)
        parcial.write_bytes(Body if isinstance(Body, bytes) else Body.read())
        os.replace(parcial, destino)
        return {}

    def upload_file(self, Filename, Bucket=None, Key: str = "", **_):
        gravar_atomico_copiando(Path(Filename), Key)

    def download_file(self, Bucket=None, Key: str = "", Filename=None, **_):
        p = caminho_local(Key)
        if not p.is_file():
            raise _erro_404("GetObject", Key)
        shutil.copyfile(p, Filename)

    def get_paginator(self, nome: str):
        if nome != "list_objects_v2":
            raise NotImplementedError(nome)
        return _Paginador()
