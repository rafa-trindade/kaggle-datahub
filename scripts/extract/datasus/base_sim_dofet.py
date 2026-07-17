"""
Módulo compartilhado pras categorias do SIM que moram na pasta "DOFET"
do FTP do DATASUS -- confirmado pelo usuário navegando direto no FTP:

  /dissemin/publicos/SIM/CID9/DOFET/  -- DOFET, DOINF (1979-1995, ano 2 dígitos)
  /dissemin/publicos/SIM/CID10/DOFET/ -- DOFET, DOINF, DOMAT, DOREXT (1996+, ano 2 dígitos)

Confirmado: mesmo os arquivos "CID10" nessa pasta usam ANO EM 2
DÍGITOS (ex: DOMAT96.DBC, DOREXT13.dbc) -- diferente da pasta DORES
(Declaração de Óbito geral), que usa 4 dígitos a partir de 1996.

Cobertura por categoria:
  DOFET (óbitos fetais)               -- CID9 + CID10, 1979-2024
  DOINF (óbitos infantis)             -- CID9 + CID10, 1979-2024
  DOMAT (óbitos maternos)             -- só CID10, 1996-2024
  DOREXT (residentes no exterior)     -- só CID10, 2013-2024
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

PASTA_BUCKET = "sim"


def criar_regra(prefixo: str):
    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        if not (nome.startswith(prefixo) and nome.endswith(".DBC")):
            return False
        ano_str = nome[len(prefixo):-4]
        return ano_str.isdigit() and len(ano_str) == 2
    return regra


def executar_fetch(prefixo: str, diretorio_ftp: str, output_subdir: str):
    """Ponto de entrada comum -- UM diretório só. Uso: categorias sem
    dado preliminar a checar (ex: DOFET/DOINF na era CID9, que não tem
    pasta PRELIM equivalente)."""
    executar_fetch_multiplos(prefixo, [diretorio_ftp], output_subdir)


def executar_fetch_multiplos(prefixo: str, diretorios_ftp: list[str], output_subdir: str):
    """Mesmo padrão usado em fetch_sim_declaracao_obito_cid10.py e
    fetch_sim_causas_externas_cid10.py -- vários diretórios (ex:
    consolidado + preliminar) caem na MESMA subpasta local/manifesto."""
    output_dir = str(LANDING_DIR / output_subdir)
    regra = criar_regra(prefixo)

    sucesso_geral = True
    houve_novidade = False

    for diretorio in diretorios_ftp:
        print(f"Sincronizando dados {prefixo} do diretório: {diretorio}")
        sucesso, novidade = sincronizar_ftp(diretorio, output_dir, regra, pasta_bucket=PASTA_BUCKET)
        sucesso_geral = sucesso_geral and sucesso
        houve_novidade = houve_novidade or novidade

    if not sucesso_geral:
        exit(exit_codes.ERRO)
    elif not houve_novidade:
        print("[INFO] Nenhum arquivo novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)