"""SIA/SUS - PA (Produção Ambulatorial, Jul/1994-atual)."""
from scripts.extract.datasus.base_sia import executar_fetch, DIRETORIO_FTP_ANTIGO, DIRETORIO_FTP_MODERNO

if __name__ == "__main__":
    # O PA tem manifesto próprio. Passar pasta_bucket explicitamente evita 
    # que o otimizador procure no manifesto errado ("sia") e acabe 
    # checando os ~10 mil arquivos na rede, um por um.
    executar_fetch(
        "PA", "dbc_sia_pa",
        diretorios_ftp=[DIRETORIO_FTP_ANTIGO, DIRETORIO_FTP_MODERNO],
        pasta_bucket="producao_ambulatorial",
    )
