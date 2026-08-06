# 📥 Baixar bases do DataHub Brasil

Utilitário para baixar bases específicas (ou todas) diretamente dos datasets do **DataHub Brasil** publicados no Kaggle, sem precisar entrar no site e baixar arquivo por arquivo.

Dois datasets são suportados:

| Chave | Dataset no Kaggle | Conteúdo |
|-------|-------------------|----------|
| `principal` | [`rafatrindade/brazilian-kaggle-datahub`](https://www.kaggle.com/datasets/rafatrindade/brazilian-kaggle-datahub) | SIM, SINASC, CNES, SIH, SIA (APACs/RAAS), CIHA, SINAN, IBGE, PNS |
| `pa` | [`rafatrindade/sia-producao-ambulatorial`](https://www.kaggle.com/datasets/rafatrindade/sia-producao-ambulatorial) | Produção Ambulatorial (SIA/PA), particionada por competência mensal |

---

## 1. Pré-requisitos

- **Python 3.9+** instalado.
- Uma **conta no Kaggle** (gratuita): <https://www.kaggle.com>.

Instale a biblioteca do Kaggle:

```bash
pip install kaggle
```

---

## 2. Obter a chave da API do Kaggle

O download usa a API do Kaggle, que exige uma credencial (um arquivo `kaggle.json`). Você gera isso uma única vez:

1. Faça login no Kaggle.
2. Acesse **<https://www.kaggle.com/settings/account>** (ou clique na sua foto → *Settings*).
3. Na seção **API**, clique em **"Create New Token"**.
4. O navegador baixa um arquivo chamado **`kaggle.json`** — ele contém seu usuário e sua chave. **Guarde-o com cuidado, é pessoal e secreto.**

---

## 3. Onde colocar o `kaggle.json`

A biblioteca procura a credencial automaticamente num local padrão, que depende do seu sistema:

### Windows

Coloque o arquivo em:

```
C:\Users\SEU_USUARIO\.kaggle\kaggle.json
```

Se a pasta `.kaggle` não existir, crie-a. Pelo PowerShell:

```powershell
mkdir $env:USERPROFILE\.kaggle
move $env:USERPROFILE\Downloads\kaggle.json $env:USERPROFILE\.kaggle\
```

### Linux / macOS

```bash
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json   # restringe a leitura ao seu usuário
```

### Alternativa: variáveis de ambiente

Se preferir não usar o arquivo, defina duas variáveis de ambiente com os valores que estão dentro do `kaggle.json`:

```bash
# Linux / macOS
export KAGGLE_USERNAME="seu_usuario"
export KAGGLE_KEY="sua_chave"
```

```powershell
# Windows (PowerShell)
$env:KAGGLE_USERNAME="seu_usuario"
$env:KAGGLE_KEY="sua_chave"
```

---

## 4. Configurar o download

Abra o arquivo **`baixar_dataset.py`** e edite a seção **CONFIGURAÇÃO** no topo:

```python
# Qual dataset baixar: "principal" ou "pa"
DATASET = "principal"

# Pasta de destino dos arquivos baixados.
DESTINO = "./dados"

# Quais arquivos baixar:
#   - lista VAZIA []  -> baixa TODAS as bases do dataset
#   - ou nomes exatos -> baixa só esses
ARQUIVOS = []

# Filtro por intervalo de competência (SOMENTE para DATASET = "pa").
# Formato AAAAMM. Deixe None para não usar. Se preenchido, ignora ARQUIVOS.
PA_DE = None
PA_ATE = None

# Descompactar os .zip após baixar? (o Kaggle entrega cada arquivo zipado)
DESCOMPACTAR = True

# Sobrescrever arquivos já existentes no destino?
SOBRESCREVER = False
```

### Exemplos de `ARQUIVOS`

```python
# Baixar tudo do dataset principal:
DATASET = "principal"
ARQUIVOS = []

# Baixar só a mortalidade CID-10:
ARQUIVOS = ["declaracoes_de_obito_cid10.parquet"]

# Baixar dengue e tuberculose:
ARQUIVOS = ["dengue.parquet", "tuberculose.parquet"]

# Baixar uma competência específica da Produção Ambulatorial:
DATASET = "pa"
ARQUIVOS = ["producao_ambulatorial_202401.parquet"]
```

### Baixar um intervalo de competências da PA (atalho)

Para o dataset `pa`, em vez de listar mês a mês em `ARQUIVOS`, você pode usar o filtro de intervalo `PA_DE` / `PA_ATE` (formato `AAAAMM`). Quando preenchido, ele tem prioridade sobre `ARQUIVOS`:

```python
DATASET = "pa"

PA_DE  = "202401"   # de janeiro/2024
PA_ATE = "202412"   # até dezembro/2024   -> baixa as 12 competências do ano

# outros exemplos:
# PA_DE, PA_ATE = "199407", "200012"   # de jul/1994 a dez/2000
# PA_DE, PA_ATE = "202403", "202403"   # só março/2024
```

Deixe `PA_DE` e `PA_ATE` como `None` (padrão) para não usar o filtro de intervalo.

> **Não sabe o nome exato dos arquivos?** Veja a seção 6 (Listar).

---

## 5. Executar

Na pasta `download-datasets`, rode:

```bash
python baixar_dataset.py
```

Os arquivos serão baixados para a pasta definida em `DESTINO` (por padrão, `./dados`), descompactados (se `DESCOMPACTAR = True`) e prontos para uso.

---

## 6. Listar os arquivos disponíveis

Para ver todos os nomes de arquivos de um dataset antes de escolher, configure o `DATASET` desejado no topo do script e rode:

```bash
python baixar_dataset.py --listar
```

Isso imprime a lista completa de bases daquele dataset, sem baixar nada.

---

## 7. Como ler os dados baixados

As bases são arquivos **Parquet** (exceto a PNS, que é `.txt`). Exemplo de leitura em Python:

```python
import pandas as pd

df = pd.read_parquet("dados/declaracoes_de_obito_cid10.parquet")
print(df.shape)
print(df.head())
```

Para a **Produção Ambulatorial**, cada arquivo é uma competência mensal. Para ler vários meses de uma vez:

```python
import pandas as pd
import glob

arquivos = glob.glob("dados/producao_ambulatorial_2024*.parquet")  # todo o ano de 2024
df = pd.concat((pd.read_parquet(f) for f in arquivos), ignore_index=True)
```

> ⚠️ **Atenção ao layout da PA:** competências de 1994-2007 têm um conjunto de colunas diferente das de 2008 em diante (mudança oficial do DATASUS). Ao concatenar as duas eras, alinhe as colunas conforme sua necessidade.

---

## Solução de problemas

| Sintoma | Causa provável | Solução |
|--------|----------------|---------|
| `Pacote 'kaggle' não instalado` | biblioteca ausente | `pip install kaggle` |
| `Falha ao autenticar` / `401` | `kaggle.json` no lugar errado ou chave inválida | Revise as seções 2 e 3; gere um novo token se preciso |
| `'X.parquet' não existe neste dataset` | nome de arquivo digitado errado | Rode `--listar` e copie o nome exato |
| Download muito lento ou interrompido | arquivo grande / rede instável | Rode de novo — arquivos já baixados são pulados (com `SOBRESCREVER = False`) |

---

Parte do projeto **[DataHub Brasil](https://github.com/rafa-trindade/kaggle-datahub)**.

