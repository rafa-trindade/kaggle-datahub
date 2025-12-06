import os
from ftplib import FTP, error_perm
from pathlib import Path

FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR = "/dissemin/publicos/SIM/PRELIM/DOFET"
OUTPUT_DIR = "data/utils/dbc_sim_causas_externas/prelim"


def ensure_output_dir(path: str):
    os.makedirs(path, exist_ok=True)


def listar_arquivos_dbc(ftp: FTP):
    arquivos = []

    def coletor(nome):
        if nome.lower().endswith(".dbc"):
            arquivos.append(nome)

    ftp.cwd(FTP_DIR)
    ftp.retrlines("NLST", coletor)
    return arquivos


def get_tamanho_ftp(ftp: FTP, nome_arquivo: str) -> int | None:
    try:
        return ftp.size(nome_arquivo)
    except error_perm:
        return None


def precisa_baixar(ftp: FTP, nome_arquivo: str, pasta_saida: str) -> bool:
    local_path = os.path.join(pasta_saida, nome_arquivo)

    if not os.path.exists(local_path):
        return True

    tamanho_local = os.path.getsize(local_path)
    tamanho_ftp = get_tamanho_ftp(ftp, nome_arquivo)

    if tamanho_ftp is None:
        print(f"[INFO] Não foi possível obter SIZE de {nome_arquivo} no FTP.")
        return False

    if tamanho_local == tamanho_ftp:
        print(f"[SKIP] {nome_arquivo} (mesmo tamanho: {tamanho_local})")
        return False
    else:
        print(f"[WARN] {nome_arquivo} existe, mas tamanho diferente "
              f"(local: {tamanho_local}, FTP: {tamanho_ftp}) -> rebaixando.")
        return True


def baixar_arquivo(ftp: FTP, nome_arquivo: str, pasta_saida: str) -> bool:
    local_path = os.path.join(pasta_saida, nome_arquivo)

    if not precisa_baixar(ftp, nome_arquivo, pasta_saida):
        return False

    print(f"[DOWN] {nome_arquivo}")
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {nome_arquivo}", f.write)

    print(f"[OK] {local_path}")
    return True


def main() -> bool:
    ensure_output_dir(OUTPUT_DIR)

    print(f"Conectando a {FTP_HOST}...")
    arquivos_baixados = False

    with FTP(FTP_HOST, timeout=60) as ftp:
        ftp.login()  
        arquivos = listar_arquivos_dbc(ftp)

        if not arquivos:
            print("Nenhum .dbc encontrado.")
            return False

        relevantes = [
            a for a in arquivos
            if a.upper().startswith("DOEXT")
            and a.upper().endswith(".DBC")
            and a[5:7].isdigit()
        ]

        print(
            f"Encontrados {len(arquivos)} arquivos .dbc. "
        )

        for arq in relevantes:
            if baixar_arquivo(ftp, arq, OUTPUT_DIR):
                arquivos_baixados = True

    print("[INFO] Arquivos novos/atualizados." if arquivos_baixados else "[INFO] Nenhuma atualização.")
    return arquivos_baixados


if __name__ == "__main__":
    updated = main()
    exit(0 if updated else 1) # código de saída 0 se houve atualização, 1 caso contrário
