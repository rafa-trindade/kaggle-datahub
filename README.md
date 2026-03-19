# ⚙️ kaggle-datahub

[![Projeto Badge](https://img.shields.io/badge/-Datasets%20Kaggle-2B5482?style=flat-square&logo=kaggle&logoColor=fff&labelColor=1D3557)](https://www.kaggle.com/rafatrindade/datasets)

Pipeline automatizado de Engenharia de Dados para extração, processamento e publicação de dados públicos brasileiros sobre **saúde, orçamento e indicadores sociais**.

O pipeline coleta dados brutos de fontes governamentais, processa em formatos estruturados usando DuckDB e Pandas, carrega em um Data Lake local no MinIO (S3) e sincroniza os datasets tratados diretamente com o Kaggle.

> 📦 Dataset público: [Brazilian Kaggle Datahub](https://www.kaggle.com/datasets/rafatrindade/brazilian-kaggle-datahub)

---

## 📊 Fontes de Dados

O pipeline integra diversas bases de dados públicas do Brasil:

| Fonte | Protocolo | Sistema | Cobertura |
|---|---|---|---|
| DATASUS | FTP | SIM/CID-10 - Declarações de óbito e causas externas | 1996–atual |
| DATASUS | FTP | SIM/CID-9 - Declarações de óbito históricas | 1979–1995 |
| DATASUS | FTP | Painel de Oncologia | 2013–atual |
| Dados Abertos MS | HTTP/ZIP | CNES - Cadastro Nacional de Estabelecimentos de Saúde | Atual |
| Dados Abertos MS | HTTP/ZIP | Macrorregiões de Saúde com geolocalização | Atual |
| SIOPS | API REST | Execução orçamentária em saúde por UF (Subfunções, RREO, Indicadores) | 2013–atual |
| IBGE | Microdados (posicional) | PNS 2013 - Pesquisa Nacional de Saúde | 2013 |
| IBGE | Microdados (posicional) | PNS 2019 - Pesquisa Nacional de Saúde | 2019 |

---

## 🛠️ Stack Tecnológico

| Camada | Tecnologia |
|---|---|
| Linguagem | Python 3.10+ |
| Processamento analítico | DuckDB |
| Manipulação de dados | Pandas |
| Armazenamento (Data Lake) | MinIO - Object Storage compatível com S3 |
| Comunicação S3 | boto3 |
| Rede / Proxy | PySocks - roteamento reverso SOCKS5 para FTP |
| Distribuição | Kaggle Python SDK (`kaggle`) |
| Configuração | python-dotenv |

---

## 🏗️ Arquitetura do Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                          FONTES EXTERNAS                            │
│  FTP DATASUS   │   API SIOPS   │   HTTP/ZIP MS   │   IBGE (manual)  │
└───────┬─────────────────┬──────────────┬──────────────────┬─────────┘
        │                 │              │                  │
        ▼                 ▼              ▼                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    1. EXTRACT  (scripts/extract/)                   │
│  .dbc · .csv · .zip · .txt  →  data/landing/                        │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    2. PROCESS  (scripts/process/)                   │
│  DuckDB SQL + Pandas  →  data/raw/                                  │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    3. LOAD  (scripts/load/)                         │
│  Upload incremental boto3  →  MinIO (S3)                            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    4. PUBLISH  (scripts/kaggle/)                    │
│  MinIO  →  ZIP local  →  Kaggle API  →  Dataset público             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Estrutura do Projeto

```
kaggle-datahub/
├── data/
│   ├── landing/                        # Arquivos brutos das fontes originais
│   │   ├── csv_cnes/
│   │   ├── csv_macroregiao/
│   │   ├── csv_siops_subfuncao/
│   │   ├── csv_siops_rreo/
│   │   ├── csv_siops_indicador/
│   │   ├── dbc_datasus_sim/
│   │   │   ├── cid9/
│   │   │   └── cid10/
│   │   ├── dbc_sim_causas_externas/
│   │   │   ├── cid9/
│   │   │   └── cid10/
│   │   └── dbc_datasus_po/
│   └── raw/                            # Dados processados prontos para o Lake
│       ├── dados_abertos/
│       │   ├── cnes_estabelecimentos_de_saude/
│       │   ├── geo_macroregiao_de_saude/
│       │   └── siops_orcamento_publico/
│       ├── datasus/
│       │   └── declaracoes_de_obito_causas_externas/
│       ├── feminicidio/
│       └── ibge/
│           └── pesquisa_nacional_de_saude/
├── scripts/
│   ├── extract/
│   │   ├── dados_abertos/
│   │   │   ├── base_dados_abertos.py           # Utilitários HTTP/ZIP compartilhados
│   │   │   ├── fetch_cnes_estabelecimentos.py
│   │   │   ├── fetch_macroregiao_de_saude.py
│   │   │   └── fetch_siops_orcamento_publico.py
│   │   └── datasus/
│   │       ├── base_ftp.py                     # Cliente FTP com retry, resume e SOCKS5
│   │       ├── fetch_painel_oncologia.py
│   │       ├── fetch_sim_causas_externas_cid9.py
│   │       ├── fetch_sim_causas_externas_cid10.py
│   │       ├── fetch_sim_declaracao_obito_cid9.py
│   │       └── fetch_sim_declaracao_obito_cid10.py
│   ├── process/
│   │   ├── dados_abertos/
│   │   │   ├── base_process_abertos.py         # Utilitário DuckDB → CSV
│   │   │   ├── process_cnes_estabelecimentos.py
│   │   │   ├── process_macroregiao_de_saude.py
│   │   │   └── process_siops_orcamento_publico.py
│   │   └── feminicidio/
│   │       ├── process_feminicidio_br.py       # Filtro CID-10 (X85–Y09, Y35)
│   │       ├── process_feminicidio_pns_2013.py # Parser posicional PNS 2013
│   │       └── process_feminicidio_pns_2019.py # Parser posicional PNS 2019
│   ├── load/
│   │   └── load_raw_to_bucket.py               # Upload incremental para MinIO
│   └── kaggle/
│       └── load_kaggle_datahub.py              # Publicação no Kaggle
├── .env
├── .kaggle/
│   └── kaggle.json
└── README.md
```

---

## ⚙️ Detalhes das Etapas

### 1. Extração (`scripts/extract/`)

Busca arquivos `.dbc`, `.csv`, `.zip` e `.txt` em APIs públicas e no servidor FTP do DATASUS, salvando os arquivos originais intactos na zona `data/landing/`.

**FTP robusto (`base_ftp.py`):**
- Download em blocos com retomada automática por byte offset (resume) - interrupções não reiniciam o download do zero
- Retry configurável (padrão: 10 tentativas com backoff de 5s)
- Verificação de integridade por comparação de tamanho local vs. remoto
- Roteamento opcional via proxy reverso SOCKS5, necessário para contornar a instabilidade e restrições de rede do FTP do DATASUS

**Proxy SOCKS5 reverso (opcional):** ative com `ssh -R 1080 -N usuario@IP_VPS` antes de executar os scripts FTP.

### 2. Processamento (`scripts/process/`)

Utiliza o **DuckDB** para executar queries SQL analíticas diretamente sobre arquivos CSV, sem necessidade de banco de dados intermediário, exportando os resultados tratados para `data/raw/`.

- **Feminicídio (SIM)**: filtra registros do sexo feminino com causas de óbito nos grupos CID-10 `X85–Y09` (agressões) e `Y35` (intervenção legal), aplica mapeamentos de variáveis categóricas e enriquece com a descrição completa da causa
- **PNS 2013 / 2019**: parser linha-a-linha de arquivos de microdados de largura fixa (positional), com dicionários completos de posições, variáveis e categorias; filtra apenas mulheres que declararam ter sofrido algum tipo de violência
- **SIOPS**: consolida múltiplos CSVs por UF e período em um único arquivo usando `read_csv_auto` com `union_by_name=true`
- **CNES**: leitura com encoding `ISO-8859-1` e tipagem `all_varchar` para preservar zeros à esquerda em códigos IBGE
- **Macrorregiões**: join entre o CSV oficial do Ministério da Saúde e planilha XLS de geolocalização via DuckDB, com padding de `cod_municipio` para 6 dígitos

### 3. Carga (`scripts/load/`)

Mapeia recursivamente o diretório `data/raw/` e executa upload **incremental** para o MinIO:
- Antes de cada arquivo, consulta o tamanho remoto via `HeadObject` - arquivos já sincronizados são ignorados
- Cria o bucket automaticamente caso não exista
- Preserva a estrutura de subdiretórios como chave S3

### 4. Publicação (`scripts/kaggle/`)

- Baixa o ecossistema completo do bucket MinIO para um diretório temporário, preservando a estrutura de pastas
- Gera o `dataset-metadata.json` exigido pela API do Kaggle com versionamento automático por data
- Detecta se o dataset já existe (tratando erros 403/404 de datasets privados) e executa `create_version` ou `create_new` conforme apropriado
- Empacota tudo em ZIP (`dir_mode='zip'`) antes do envio

---

## 🚀 Instalação e Execução

### Pré-requisitos

```bash
pip install requests pandas duckdb boto3 kaggle python-dotenv openpyxl PySocks
```

### Variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
MINIO_ENDPOINT=http://localhost:9000
MINIO_ROOT_USER=seu_usuario
MINIO_ROOT_PASSWORD=sua_senha
MINIO_BUCKET=nome_do_bucket
```

### Credenciais do Kaggle

Coloque o arquivo `kaggle.json` em `.kaggle/kaggle.json` na raiz do projeto:

```json
{
  "username": "seu_usuario_kaggle",
  "key": "sua_api_key"
}
```

### Executando o pipeline

**Etapa 1 - Extração**

```bash
# DATASUS via FTP
python -m scripts.extract.datasus.fetch_sim_declaracao_obito_cid10
python -m scripts.extract.datasus.fetch_sim_declaracao_obito_cid9
python -m scripts.extract.datasus.fetch_sim_causas_externas_cid10
python -m scripts.extract.datasus.fetch_sim_causas_externas_cid9
python -m scripts.extract.datasus.fetch_painel_oncologia

# Dados Abertos e SIOPS via HTTP/API
python -m scripts.extract.dados_abertos.fetch_cnes_estabelecimentos
python -m scripts.extract.dados_abertos.fetch_macroregiao_de_saude
python -m scripts.extract.dados_abertos.fetch_siops_orcamento_publico
```

> ⚠️ **PNS 2013 e 2019**: os microdados devem ser baixados manualmente no [site do IBGE](https://www.ibge.gov.br/estatisticas/sociais/saude/9160-pesquisa-nacional-de-saude.html) e salvos em `data/raw/ibge/pesquisa_nacional_de_saude/` como `PNS_2013.txt` e `PNS_2019.txt`.
>
> ⚠️ **Macrorregiões**: o arquivo `macro_geolocalizacao.xls` deve estar presente em `data/landing/csv_macroregiao/` antes de executar o processamento.

**Etapa 2 - Processamento**

```bash
python -m scripts.process.dados_abertos.process_cnes_estabelecimentos
python -m scripts.process.dados_abertos.process_macroregiao_de_saude
python -m scripts.process.dados_abertos.process_siops_orcamento_publico
python -m scripts.process.feminicidio.process_feminicidio_br
python -m scripts.process.feminicidio.process_feminicidio_pns_2013
python -m scripts.process.feminicidio.process_feminicidio_pns_2019
```

**Etapa 3 - Carga no Data Lake**

```bash
python -m scripts.load.load_raw_to_bucket
```

**Etapa 4 - Publicação no Kaggle**

```bash
python -m scripts.kaggle.load_kaggle_datahub
```

---

## 📝 Notas Técnicas

- **Bug do Milênio no FTP**: os scripts de extração do SIM tratam nomes de arquivo com ano em 2 dígitos (ex: `DOEXT96.DBC` → 1996, `DOEXT05.DBC` → 2005) usando limiares ajustados por série histórica - CID-9 usa limiar `>= 79`, CID-10 usa `>= 90`.
- **SIOPS paginado**: a API retorna até 10.000 registros por requisição com `page=0&size=10000`. O script mantém estado por arquivo CSV existente na pasta de saída, permitindo reinicialização a partir do último período já baixado.
- **Arquivos ignorados no upload**: `.gitkeep` e `raw_lake_metadados.csv` são excluídos automaticamente da publicação no Kaggle.
- **Proxy SOCKS5**: configurado via `socks.set_default_proxy` em `base_ftp.py`. Para desativar permanentemente, remova o bloco de importação e configuração do `socks` no início do arquivo.

---

## 📄 Licença

Os dados publicados estão sob [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/) - domínio público, sem restrições de uso.

Os scripts deste repositório estão sob [MIT License](LICENSE).