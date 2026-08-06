"""SIA/SUS - PA (Produção Ambulatorial, Jul/1994-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch, DIRETORIO_FTP_ANTIGO, DIRETORIO_FTP_MODERNO

if __name__ == "__main__":
    executar_fetch("PA", "dbc_sia_pa", diretorios_ftp=[DIRETORIO_FTP_ANTIGO, DIRETORIO_FTP_MODERNO])
