"""
Baixa bases específicas (ou todas) dos datasets do DataHub Brasil no Kaggle.

Dois datasets são suportados:
  - principal : rafatrindade/brazilian-kaggle-datahub   (SIM, SINASC, CNES, SIH, SIA-APACs, CIHA, SINAN, IBGE, PNS...)
  - pa        : rafatrindade/sia-producao-ambulatorial  (Produção Ambulatorial, particionada por competência)

Configure a seção "CONFIGURAÇÃO" abaixo e rode:

    python baixar_dataset.py

Veja o README.md (nesta pasta) para o passo a passo completo, inclusive
como obter e onde colocar a chave da API do Kaggle.
"""
import os
import sys

# ==========================================================================
# CONFIGURAÇÃO
# ==========================================================================

# Qual dataset baixar: "principal" ou "pa"
DATASET = "principal"

# Pasta de destino dos arquivos baixados.
DESTINO = "./dados"

# Quais arquivos baixar.
#   - Deixe a lista VAZIA ([]) para baixar TODAS as bases do dataset.
#   - Ou liste os nomes exatos dos arquivos desejados (com extensão).
# Exemplos:
#   ARQUIVOS = []                                        # tudo
#   ARQUIVOS = ["declaracoes_de_obito_cid10.parquet"]    # só uma base
#   ARQUIVOS = ["dengue.parquet", "tuberculose.parquet"] # várias
#   (dataset "pa") ARQUIVOS = ["producao_ambulatorial_202401.parquet"]
ARQUIVOS = []

# --- Filtro por intervalo de competência (SOMENTE para DATASET = "pa") ---
# Atalho para baixar um intervalo de meses da Produção Ambulatorial sem listar
# arquivo por arquivo. Use competências no formato AAAAMM (ano+mês, 6 dígitos).
#   - Deixe ambos como None para NÃO usar o filtro de intervalo.
#   - Se preenchidos, têm prioridade sobre ARQUIVOS (que é ignorado).
# Exemplos:
#   PA_DE, PA_ATE = "202401", "202412"   # todo o ano de 2024
#   PA_DE, PA_ATE = "199407", "200012"   # de jul/1994 a dez/2000
#   PA_DE, PA_ATE = "202403", "202403"   # só março/2024
PA_DE = None
PA_ATE = None

# Descompactar os .zip após baixar? (o Kaggle entrega cada arquivo zipado)
DESCOMPACTAR = True

# Sobrescrever arquivos que já existem no destino?
SOBRESCREVER = False

# ==========================================================================
# Fim da configuração -- daqui para baixo não precisa editar.
# ==========================================================================

DATASETS = {
    "principal": "rafatrindade/brazilian-kaggle-datahub",
    "pa": "rafatrindade/sia-producao-ambulatorial",
}


def _autenticar():
    """Autentica na API do Kaggle. A credencial vem do kaggle.json ou das
    variáveis de ambiente KAGGLE_USERNAME / KAGGLE_KEY (ver README)."""
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError:
        sys.exit(
            "Pacote 'kaggle' não instalado. Rode:  pip install kaggle\n"
            "(veja o README.md nesta pasta)."
        )
    api = KaggleApi()
    try:
        api.authenticate()
    except Exception as e:
        sys.exit(
            f"Falha ao autenticar na API do Kaggle: {e}\n\n"
            "Verifique se o kaggle.json está no lugar certo ou se as variáveis "
            "KAGGLE_USERNAME/KAGGLE_KEY estão definidas. Passo a passo no README.md."
        )
    return api


def _listar_arquivos(api, dataset_id):
    """Retorna a lista de nomes de arquivos disponíveis no dataset."""
    try:
        resp = api.dataset_list_files(dataset_id)
        return [f.name for f in resp.files]
    except Exception as e:
        sys.exit(f"Não consegui listar os arquivos de {dataset_id}: {e}")


def _competencias_no_intervalo(disponiveis, de, ate):
    """Filtra os arquivos de PA cujo AAAAMM (extraído do nome) está em [de, ate].

    Nomes esperados: producao_ambulatorial_AAAAMM.parquet. Como AAAAMM tem ano
    de 4 dígitos, a comparação lexicográfica de strings já equivale à ordem
    cronológica.
    """
    import re
    selec = []
    for nome in disponiveis:
        m = re.search(r"(\d{6})\.parquet$", nome)
        if m and de <= m.group(1) <= ate:
            selec.append(nome)
    return sorted(selec)


def _validar_intervalo_pa(de, ate):
    import re
    for rot, val in (("PA_DE", de), ("PA_ATE", ate)):
        if not re.fullmatch(r"\d{6}", str(val)):
            sys.exit(f"{rot} inválido: '{val}'. Use o formato AAAAMM (ex.: 202401).")
    if de > ate:
        sys.exit(f"Intervalo invertido: PA_DE ({de}) é maior que PA_ATE ({ate}).")


def main():
    if DATASET not in DATASETS:
        sys.exit(f"DATASET inválido: '{DATASET}'. Use 'principal' ou 'pa'.")
    dataset_id = DATASETS[DATASET]

    api = _autenticar()
    os.makedirs(DESTINO, exist_ok=True)

    disponiveis = _listar_arquivos(api, dataset_id)
    print(f"Dataset: {dataset_id}")
    print(f"Arquivos disponíveis: {len(disponiveis)}")

    # decide o que baixar
    usar_intervalo_pa = PA_DE is not None or PA_ATE is not None
    if usar_intervalo_pa and DATASET != "pa":
        print("[AVISO] PA_DE/PA_ATE só valem para DATASET='pa' -- ignorados aqui.")
        usar_intervalo_pa = False

    if usar_intervalo_pa:
        if PA_DE is None or PA_ATE is None:
            sys.exit("Para usar o intervalo, preencha PA_DE e PA_ATE (ambos).")
        _validar_intervalo_pa(PA_DE, PA_ATE)
        alvo = _competencias_no_intervalo(disponiveis, PA_DE, PA_ATE)
        print(f"Filtro de competência: {PA_DE} a {PA_ATE} -> {len(alvo)} arquivo(s).")
        if not alvo:
            sys.exit("Nenhuma competência encontrada nesse intervalo. "
                     "Confira PA_DE/PA_ATE e o que o dataset já tem publicado.")
    elif not ARQUIVOS:
        alvo = disponiveis
        print("Baixando TODAS as bases (ARQUIVOS vazio).")
    else:
        alvo = []
        for nome in ARQUIVOS:
            if nome in disponiveis:
                alvo.append(nome)
            else:
                print(f"  [AVISO] '{nome}' não existe neste dataset -- ignorado.")
        if not alvo:
            sys.exit("Nenhum dos arquivos pedidos existe no dataset. "
                     "Confira os nomes (use LISTAR abaixo) e tente de novo.")

    print(f"Serão baixados {len(alvo)} arquivo(s) para: {os.path.abspath(DESTINO)}\n")

    for i, nome in enumerate(alvo, 1):

        subpasta = os.path.dirname(nome)
        destino_pasta = os.path.join(DESTINO, subpasta) if subpasta else DESTINO
        destino_final = os.path.join(DESTINO, nome)
        if os.path.exists(destino_final) and not SOBRESCREVER:
            print(f"[{i}/{len(alvo)}] {nome} -- já existe, pulando (SOBRESCREVER=False).")
            continue
        os.makedirs(destino_pasta, exist_ok=True)
        print(f"[{i}/{len(alvo)}] Baixando {nome} ...")
        try:
            api.dataset_download_file(
                dataset_id, nome, path=destino_pasta,
                force=SOBRESCREVER, quiet=False,
            )
        except Exception as e:
            print(f"    [ERRO] falha ao baixar {nome}: {e}")
            continue

    if DESCOMPACTAR:
        import zipfile
        import glob
        zips = glob.glob(os.path.join(DESTINO, "**", "*.zip"), recursive=True)
        for z in zips:
            try:
                with zipfile.ZipFile(z) as zf:
                    zf.extractall(os.path.dirname(z))
                os.remove(z)
                print(f"  descompactado: {os.path.relpath(z, DESTINO)}")
            except zipfile.BadZipFile:
                print(f"  [AVISO] {os.path.basename(z)} não é um zip válido -- mantido.")

    print("\nConcluído.")


def listar():
    """Modo utilitário: só lista os arquivos do dataset configurado e sai.
    Rode com:  python baixar_dataset.py --listar
    """
    dataset_id = DATASETS.get(DATASET)
    if not dataset_id:
        sys.exit(f"DATASET inválido: '{DATASET}'.")
    api = _autenticar()
    nomes = _listar_arquivos(api, dataset_id)
    print(f"Arquivos em {dataset_id} ({len(nomes)}):\n")
    for n in sorted(nomes):
        print(f"  {n}")


if __name__ == "__main__":
    if "--listar" in sys.argv or "-l" in sys.argv:
        listar()
    else:
        main()
