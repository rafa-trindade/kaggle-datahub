"""
SINASC - Sistema de Informações sobre Nascidos Vivos

Baixa arquivos .dbc do FTP do DATASUS -- Declarações de Nascido Vivo,
série histórica. Mesma estrutura de FTP/DBC do SIM (base_ftp.py).

Dois diretórios confirmados no FTP do DATASUS (mesma convenção usada em
outros sistemas, ex: CNES/200508_/):
  ANT/DNRES        -- dados ANTIGOS (pré-1996), ano em 2 dígitos: DN{UF}{AA}.dbc
  1996_/Dados/DNRES -- dados NOVOS (1996+), ano em 4 dígitos: DN{UF}{AAAA}.dbc

Achado real: existe também um caminho "NOV/DNRES" com o mesmo padrão de
nome, mas ele está desatualizado/incompleto (confirmado: só tinha 1 dos
11 arquivos de DNEX que "1996_/Dados/DNRES" tem) -- usar sempre
"1996_/Dados/DNRES", não "NOV/DNRES".

Os dois caem na mesma pasta local/bucket -- é a mesma série histórica,
só dividida pelo DATASUS em dois formatos de nome de arquivo.

Achado real: o DATASUS também publica um arquivo consolidado NACIONAL
(DNBR{ano}.dbc) ao lado dos arquivos por estado a partir de 2014 -- se
não for filtrado, duplica todo nascimento de 2014+ (uma vez no arquivo
do estado, outra no nacional). "BR" não é UF -- o filtro abaixo só
aceita as 27 UFs reais.

Nascidos no Exterior (DNEX) são uma fonte SEPARADA, não incluída aqui
-- ver fetch_sinasc_dnex.py.
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

OUTPUT_DIR = str(LANDING_DIR / "dbc_sinasc")
PASTA_BUCKET = "sinasc"

UFS_VALIDAS = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG",
    "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR",
    "RS", "SC", "SE", "SP", "TO",
}


def regra_dn_novo(nome_arquivo: str) -> bool:
    """DN{UF}{AAAA}.dbc -- ano em 4 dígitos (1996+)."""
    nome = nome_arquivo.upper()
    if not (nome.startswith("DN") and nome.endswith(".DBC")):
        return False
    uf = nome[2:4]
    ano_str = nome[4:-4]
    return uf in UFS_VALIDAS and ano_str.isdigit() and len(ano_str) == 4


def regra_dn_antigo(nome_arquivo: str) -> bool:
    """DN{UF}{AA}.dbc -- ano em 2 dígitos (pré-1996). Sem ambiguidade
    de século (SINASC não existia antes de ~1990), então todo ano de 2
    dígitos aqui é 19XX."""
    nome = nome_arquivo.upper()
    if not (nome.startswith("DN") and nome.endswith(".DBC")):
        return False
    uf = nome[2:4]
    ano_str = nome[4:-4]
    return uf in UFS_VALIDAS and ano_str.isdigit() and len(ano_str) == 2


FONTES_FTP = [
    {
        "diretorio": "/dissemin/publicos/SINASC/ANT/DNRES",
        "regra": regra_dn_antigo,
        "tipo": "Antigos (pré-1996)",
    },
    {
        "diretorio": "/dissemin/publicos/SINASC/1996_/Dados/DNRES",
        "regra": regra_dn_novo,
        "tipo": "Novos (1996+)",
    },
]

if __name__ == "__main__":
    sucesso_geral = True
    houve_novidade = False

    for fonte in FONTES_FTP:
        print(f"Sincronizando dados {fonte['tipo']} do diretório: {fonte['diretorio']}")

        sucesso, novidade = sincronizar_ftp(fonte["diretorio"], OUTPUT_DIR, fonte["regra"], pasta_bucket=PASTA_BUCKET)
        sucesso_geral = sucesso_geral and sucesso
        houve_novidade = houve_novidade or novidade

    if not sucesso_geral:
        exit(exit_codes.ERRO)
    elif not houve_novidade:
        print("[INFO] Nenhum arquivo novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)