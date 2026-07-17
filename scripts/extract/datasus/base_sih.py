"""
Módulo compartilhado pras 3 fontes do SIH/SUS (RD, RJ, SP) -- todas
vivem na mesma pasta FTP, diferenciadas só pelo prefixo do arquivo:

  RD{UF}{AAMM}.dbc -- AIH Reduzida (internações aprovadas p/ pagamento)
  RJ{UF}{AAMM}.dbc -- AIH Rejeitadas
  SP{UF}{AAMM}.dbc -- Serviços Profissionais (atos médicos)

Confirmado (múltiplas fontes independentes, incluindo o pacote R
microdatasus): série moderna a partir de 2008 fica em
SIHSUS/200801_/Dados/ -- só essa série é usada aqui (a série antiga,
1992-2007, foi deixada de fora por decisão do usuário, dado o volume
total envolvido).

Diferente do CNES (retrato do mês mais recente): SIH é série histórica
que ACUMULA mês a mês, mesmo padrão do SIM -- usa merge incremental,
não substituição completa.
"""
from scripts.extract.datasus.base_ftp import sincronizar_ftp
from scripts.common.paths import LANDING_DIR
from scripts.common import exit_codes

DIRETORIO_FTP = "/dissemin/publicos/SIHSUS/200801_/Dados"
PASTA_BUCKET = "sih"


def criar_regra(prefixo: str):
    def regra(nome_arquivo: str) -> bool:
        nome = nome_arquivo.upper()
        if not (nome.startswith(prefixo) and nome.endswith(".DBC")):
            return False
        resto = nome[len(prefixo):-4]
        # resto = UF (2 letras) + AAMM (4 dígitos) = 6 caracteres
        return len(resto) == 6 and resto[:2].isalpha() and resto[2:].isdigit()
    return regra


def executar_fetch(prefixo: str, output_subdir: str):
    output_dir = str(LANDING_DIR / output_subdir)
    regra = criar_regra(prefixo)

    print(f"Sincronizando dados {prefixo} do diretório: {DIRETORIO_FTP}")
    sucesso, novidade = sincronizar_ftp(DIRETORIO_FTP, output_dir, regra, pasta_bucket=PASTA_BUCKET)

    if not sucesso:
        exit(exit_codes.ERRO)
    elif not novidade:
        print("[INFO] Nenhum arquivo novo desde a última execução.")
        exit(exit_codes.SEM_NOVIDADE)
    else:
        exit(exit_codes.SUCESSO)