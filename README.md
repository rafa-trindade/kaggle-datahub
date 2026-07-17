![header](docs/images/datahub-banner.png)

[![License: MIT](https://img.shields.io/badge/License-MIT-346B5D?labelColor=123C2F)](LICENSE)
[![Kaggle](https://img.shields.io/badge/Dataset-Kaggle-346B5D?labelColor=123C2F&logo=kaggle&logoColor=ffffff)](https://www.kaggle.com/datasets/rafatrindade/brazilian-kaggle-datahub)
[![GitHub Stars](https://img.shields.io/github/stars/rafa-trindade/kaggle-datahub?style=flat&labelColor=123C2F&color=346B5D)](https://github.com/rafa-trindade/kaggle-datahub)

O **DataHub Brasil** nasce de uma necessidade prática: dados públicos brasileiros de altíssimo valor existem, são gratuitos e são oficiais - mas estão espalhados entre sistemas diferentes do DATASUS e do IBGE, cada um com seu próprio protocolo de acesso, formato de arquivo e convenção de nomenclatura. Reunir qualquer análise minimamente ampla exige garimpar meia dúzia de fontes antes de escrever a primeira linha de código de análise.

Este é um hub **bruto e geral** de dados públicos do Brasil: mortalidade, nascimentos, rede assistencial completa (estabelecimentos, habilitações, leitos, profissionais, equipamentos), internações hospitalares, dezenas de doenças de notificação compulsória, população e PIB por município, e os microdados completos da Pesquisa Nacional de Saúde - sem recorte temático, sem filtro de especialidade, sem viés de pesquisa específica. A ideia é justamente o oposto de um recorte: publicar cada sistema por completo, do jeito mais próximo possível do dado oficial, para que qualquer pesquisador possa aplicar seu próprio filtro.

---

## 📊 Fontes de Dados e Escopo

### **1. Mortalidade (Fonte: SIM - DATASUS)**

O **Sistema de Informações sobre Mortalidade (SIM)** consolida as Declarações de Óbito de todo o país desde 1979. Aqui, o SIM é publicado **por completo**, em todos os seus subsistemas, sem recorte por causa de óbito.

**Escopo e Processamento:** São baixados via FTP público do DATASUS os arquivos `.dbc` de cada subsistema, nas eras CID-9 (1979-1995) e CID-10 (1996-atual, quando aplicável), convertidos para Parquet e mesclados incrementalmente - execuções futuras só baixam e reprocessam o que for novo, sem reprocessar o histórico inteiro. Descoberta importante durante a construção: os subsistemas de Causas Externas, Óbitos Fetais, Óbitos Infantis e Óbitos Maternos não ficam em pastas próprias no FTP do DATASUS - todos dividem a mesma pasta física (nomeada "DOFET" por herança histórica), diferenciados só pelo prefixo do nome do arquivo.

**Bases disponibilizadas:**

- `declaracoes_de_obito_cid9.parquet` / `declaracoes_de_obito_cid10.parquet` - Declarações de óbito, todas as causas, 1979-1995 e 1996-atual.
- `declaracoes_de_obito_causas_externas_cid9.parquet` / `_cid10.parquet` - Óbitos por causas externas (acidentes, violência).
- `declaracoes_de_obito_fetais_cid9.parquet` / `_cid10.parquet` - Óbitos fetais.
- `declaracoes_de_obito_infantis_cid9.parquet` / `_cid10.parquet` - Óbitos infantis.
- `declaracoes_de_obito_maternos_cid10.parquet` - Óbitos maternos (só existe a partir de 1996, sem era CID-9).
- `declaracoes_de_obito_residentes_exterior_cid10.parquet` - Óbitos de brasileiros residentes no exterior (só a partir de 2013).

> BRASIL. Ministério da Saúde. DATASUS. *Sistema de Informações sobre Mortalidade (SIM)*. Brasília, DF: Ministério da Saúde. Disponível em: <https://datasus.saude.gov.br/mortalidade-desde-1996-pela-cid-10>.

---

### **2. Nascimentos (Fonte: SINASC - DATASUS)**

O **Sistema de Informações sobre Nascidos Vivos (SINASC)** é o equivalente do SIM para nascimentos - a base oficial de natalidade do Brasil desde 1996 (com dados pré-1996 também disponíveis).

**Escopo e Processamento:** Baixado via FTP público do DATASUS, com uma descoberta relevante durante a construção: a pasta `SINASC/NOV/DNRES` (aparentemente o caminho "principal") está **desatualizada** em relação à pasta `SINASC/1996_/Dados/DNRES`, que é a de fato mantida corrente - o pipeline usa a segunda. Os arquivos consolidados nacionais por ano (`DNBR{AAAA}.dbc`) são deliberadamente excluídos do processamento por duplicarem integralmente os nascimentos já presentes nos arquivos por UF (confirmado empiricamente, contagem exata).

**Bases disponibilizadas:**

- `declaracoes_de_nascido_vivo.parquet` - Nascidos vivos por UF, 1994-atual.
- `declaracoes_de_nascido_vivo_exterior.parquet` - Brasileiros nascidos no exterior, registrados no sistema.

> BRASIL. Ministério da Saúde. DATASUS. *Sistema de Informações sobre Nascidos Vivos (SINASC)*. Brasília, DF: Ministério da Saúde. Disponível em: <https://datasus.saude.gov.br/nascidos-vivos-desde-1994/>.

---

### **3. Rede Assistencial Completa (Fonte: CNES - DATASUS)**

O **Cadastro Nacional de Estabelecimentos de Saúde (CNES)** é o registro oficial de todos os estabelecimentos de saúde do Brasil. Aqui, cada arquivo do CNES é publicado **por completo, sem filtro de especialidade**, mantendo a granularidade original de cada sistema.

**Escopo e Processamento:** O cadastro de Estabelecimentos vem via HTTP/ZIP dos Dados Abertos do Ministério da Saúde. Habilitações, Leitos, Profissionais e Equipamentos vêm via FTP, organizados por UF e competência - como o CNES é um **retrato** (não uma série histórica que acumula), cada nova competência **substitui por completo** a anterior, ao contrário do padrão de mesclagem incremental usado no SIM/SINASC.

**Bases disponibilizadas:**

- `estabelecimentos_de_saude.parquet` - Cadastro geral: identificação, endereço, CNPJ, infraestrutura.
- `habilitacoes.parquet` - Todas as habilitações de todos os estabelecimentos, todas as especialidades.
- `leitos.parquet` - Contagem de leitos por estabelecimento e tipo.
- `profissionais.parquet` - Profissionais de saúde cadastrados (CBO, carga horária, forma de contratação).
- `equipamentos.parquet` - Equipamentos cadastrados por estabelecimento (raio-X, ressonância, tomógrafo etc).

> BRASIL. Ministério da Saúde. DATASUS. *Cadastro Nacional de Estabelecimentos de Saúde (CNES)*. Brasília, DF: Ministério da Saúde. Disponível em: <https://cnes.datasus.gov.br/>.

---

### **4. Internações Hospitalares (Fonte: SIH/SUS - DATASUS)**

O **Sistema de Informações Hospitalares (SIH/SUS)** registra todas as internações realizadas pelo SUS, com diagnósticos, procedimentos, valores e tempo de permanência.

**Escopo e Processamento:** Cobre a série moderna (2008-atual) - a série anterior (1992-2007) foi deixada de fora por decisão de escopo, dado o volume já considerável só na série recente (~6.000 arquivos por subsistema, um por UF/mês). São publicados os 3 subsistemas que compõem o SIH: internações aprovadas, rejeitadas, e os atos médicos associados.

**Bases disponibilizadas:**

- `aih_reduzida.parquet` - Internações aprovadas para pagamento pelo SUS (RD).
- `aih_rejeitada.parquet` - Internações rejeitadas para pagamento (RJ).
- `servicos_profissionais.parquet` - Atos médicos realizados durante as internações (SP).

> BRASIL. Ministério da Saúde. DATASUS. *Sistema de Informações Hospitalares do SUS (SIH/SUS)*. Brasília, DF: Ministério da Saúde. Disponível em: <https://datasus.saude.gov.br/acesso-a-informacao/producao-hospitalar-sih-sus/>.

---

### **5. Doenças de Notificação Compulsória (Fonte: SINAN - DATASUS)**

O **Sistema de Informação de Agravos de Notificação (SINAN)** registra todas as doenças e agravos de notificação obrigatória no Brasil - de arboviroses a doenças ocupacionais, de violência interpessoal a doenças quase erradicadas mantidas sob vigilância ativa.

**Escopo e Processamento:** Diferente do SIM/SIH, o SINAN não é dividido por UF - cada agravo tem um único arquivo por ano, nível Brasil. São cobertos **58 agravos**, cada um publicado como um Parquet **independente** (agravos diferentes têm estruturas de campos completamente diferentes entre si, então misturar tudo numa tabela só não faria sentido). A lista completa de agravos e seus respectivos códigos está documentada em `scripts/config/agravos_sinan.py` no repositório de código. Entre os destaques: as três arboviroses (dengue, chikungunya, zika), tuberculose, hanseníase, sífilis (adquirida/congênita/gestante), HIV e AIDS (notificados como 6 sistemas separados - adulto/criança/gestante para cada), e violência interpessoal/autoprovocada, publicada aqui **sem nenhum filtro** (todos os desfechos, todos os gêneros).

**Bases disponibilizadas:** 58 arquivos Parquet, um por agravo, nomeados de forma legível (ex: `dengue.parquet`, `tuberculose.parquet`, `violencia_interpessoal_autoprovocada.parquet`, `hiv_gestante.parquet`) - não pelas siglas técnicas do DATASUS.

> BRASIL. Ministério da Saúde. DATASUS. *Sistema de Informação de Agravos de Notificação (SINAN)*. Brasília, DF: Ministério da Saúde. Disponível em: <https://datasus.saude.gov.br/sinan/>.

---

### **6. Demografia e Economia Municipal (Fonte: IBGE, via API SIDRA)**

O **IBGE**, via sua API pública SIDRA, disponibiliza séries anuais de população estimada e produto interno bruto por município.

**Escopo e Processamento:** Ambas as séries são obtidas ano a ano via API (não por download de arquivo), com descoberta dinâmica dos períodos realmente disponíveis em cada tabela - o IBGE costuma trocar o número da tabela quando muda a metodologia de cálculo (confirmado empiricamente ao longo da construção: uma tentativa inicial usou uma tabela que só cobria nível Brasil, não municipal).

**Bases disponibilizadas:**

- `populacao_estimada.parquet` - População estimada por município, 2001-atual (com lacunas nos anos de Censo/Contagem, quando a estimativa regular é substituída).
- `pib_municipal.parquet` - Produto Interno Bruto por município, 2002-atual.

> INSTITUTO BRASILEIRO DE GEOGRAFIA E ESTATÍSTICA (IBGE). *Sistema IBGE de Recuperação Automática (SIDRA)*. Rio de Janeiro: IBGE. Disponível em: <https://sidra.ibge.gov.br/>.

---

### **7. Microdados Completos da PNS (Fonte: IBGE)**

A **Pesquisa Nacional de Saúde (PNS)** é um inquérito domiciliar do IBGE com mais de 1.000 variáveis por edição, cobrindo desde diagnósticos autorreferidos até hábitos de vida e acesso a serviços de saúde.

**Escopo e Processamento:** Aqui os microdados de posição fixa são publicados **exatamente como o IBGE distribui** - sem decodificar nenhuma variável, sem recorte temático. Mapear as mais de 1.000 posições de cada edição não agregaria valor suficiente para este hub geral; quem for usar precisa do dicionário oficial de posições do IBGE para decodificar campo a campo.

**Bases disponibilizadas:**

- `microdados_pns_2013.txt` / `microdados_pns_2019.txt` - Microdados brutos de posição fixa, tal como distribuídos pelo IBGE.

*Observação: por serem arquivos volumosos e sujeitos aos termos de uso de download do IBGE, os microdados brutos são obtidos manualmente, não via automação.*

> INSTITUTO BRASILEIRO DE GEOGRAFIA E ESTATÍSTICA (IBGE). *Pesquisa Nacional de Saúde (PNS)*. Rio de Janeiro: IBGE. Disponível em: <https://www.ibge.gov.br/estatisticas/sociais/saude/9160-pesquisa-nacional-de-saude.html>.

---

### **8. Base Auxiliar (Macrorregião de Saúde)**

Para permitir cruzamentos geográficos entre as demais bases, o projeto conta com uma base auxiliar de referência, construída a partir de dados abertos do Ministério da Saúde.

**Escopo e Processamento:** O arquivo de municípios (Dados Abertos da Saúde) é combinado, via join no código do município (com correção de zero à esquerda), com um arquivo complementar de geolocalização.

**Base disponibilizada:**

- `macroregiao_de_saude.parquet` - Municípios brasileiros associados às suas macrorregiões de saúde, regiões de saúde e coordenadas geográficas.

---

## 🗓️ Cobertura Histórica

- **SIM (mortalidade):** 1979-atual, todos os 6 subsistemas.
- **SINASC (nascimentos):** 1994-atual.
- **CNES (rede assistencial):** retrato da competência mais recente disponível (não histórico).
- **SIH/SUS (internações):** 2008-atual (série moderna).
- **SINAN (agravos):** varia por agravo, geralmente a partir dos anos 2000; consultar `agravos_sinan.py` para o início exato de cada um.
- **IBGE (população/PIB):** população desde 2001, PIB desde 2002.
- **PNS/IBGE:** edições pontuais de 2013 e 2019.

---

## 🔄 Atualização e Confiabilidade

- **SIM, SINASC, CNES, SIH, SINAN:** sincronização totalmente automatizada via FTP, com detecção de novidade real (por tamanho de arquivo) antes de reprocessar ou publicar.
- **IBGE (População/PIB):** sincronização automatizada via API, ano a ano, com descoberta dinâmica de quais anos a tabela realmente cobre.
- **PNS/IBGE:** obtenção do microdado bruto é manual; a publicação (upload, sem transformação) é automatizada.
- **Macrorregião de Saúde:** sincronização automatizada via HTTP.

O pipeline só publica uma nova versão (bucket + Kaggle) quando pelo menos uma fonte automatizada reporta dado novo de verdade.

---

## 📁 Estrutura de Pastas do Dataset

```
sim/
  declaracoes_de_obito_cid9.parquet
  declaracoes_de_obito_cid10.parquet
  declaracoes_de_obito_causas_externas_cid9.parquet
  declaracoes_de_obito_causas_externas_cid10.parquet
  declaracoes_de_obito_fetais_cid9.parquet
  declaracoes_de_obito_fetais_cid10.parquet
  declaracoes_de_obito_infantis_cid9.parquet
  declaracoes_de_obito_infantis_cid10.parquet
  declaracoes_de_obito_maternos_cid10.parquet
  declaracoes_de_obito_residentes_exterior_cid10.parquet

sinasc/
  declaracoes_de_nascido_vivo.parquet
  declaracoes_de_nascido_vivo_exterior.parquet

cnes/
  estabelecimentos_de_saude.parquet
  habilitacoes.parquet
  leitos.parquet
  profissionais.parquet
  equipamentos.parquet

sih/
  aih_reduzida.parquet
  aih_rejeitada.parquet
  servicos_profissionais.parquet

sinan/
  dengue.parquet, tuberculose.parquet, hanseniase.parquet, ...
  (58 arquivos no total -- lista completa em scripts/config/agravos_sinan.py)

geo/
  macroregiao_de_saude.parquet

ibge/
  populacao_estimada.parquet
  pib_municipal.parquet
  microdados_pns_2013.txt
  microdados_pns_2019.txt
```

---

## 🛠️ Stack Tecnológico

| Camada | Tecnologia |
|---|---|
| Linguagem | Python 3.11 |
| Processamento analítico | DuckDB |
| Manipulação de dados | Pandas |
| Armazenamento (Data Lake) | MinIO - Object Storage compatível com S3 |
| Comunicação S3 | boto3 |
| Distribuição | Kaggle Python SDK (`kaggle`) |
| Configuração | python-dotenv |

---

## 🏗️ Arquitetura do Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                          FONTES EXTERNAS                            │
│      FTP DATASUS       │      API SIDRA/IBGE     │   HTTP/ZIP MS    │
└───────────┬─────────────────────────┬───────────────────┬───────────┘
            │                         │                   │
            ▼                         ▼                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  1. EXTRACT  (scripts/extract/)                     │
│         .dbc · .json · .zip  →  data/landing/ (scratch temp)        │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  2. PROCESS  (scripts/process/)                     │
│     DuckDB SQL + Pandas → Parquet → upload direto → MinIO (S3)      │
│         Manifesto por pasta_bucket detecta novidade real            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 3. PUBLISH  (scripts/kaggle/)                       │
│       MinIO  →  ZIP local  →  Kaggle API  →  Dataset público        │
└─────────────────────────────────────────────────────────────────────┘
```

**Diferença importante em relação a um pipeline tradicional:** não existe uma pasta local persistida com o histórico completo de dados brutos. `data/landing/` é puramente um scratch space temporário - cada arquivo é baixado, processado e enviado direto ao bucket, com o local sendo apagado logo em seguida. A detecção de "isso já existe, não precisa reprocessar" é feita comparando contra um **manifesto** (`_manifest.json`) mantido no próprio bucket, não contra disco local.

---

### Executando o pipeline

> ⚠️ **PNS 2013 e 2019**: os microdados devem ser baixados manualmente no [site do IBGE](https://www.ibge.gov.br/estatisticas/sociais/saude/9160-pesquisa-nacional-de-saude.html) e salvos em `data/landing/ibge/` como `PNS_2013.txt` e `PNS_2019.txt`.
>
> ⚠️ **Macrorregiões**: o arquivo `macro_geolocalizacao.xls` deve estar presente em `data/landing/csv_macroregiao/` antes de executar o processamento.

A lista completa de fontes, com seus respectivos módulos de extração e processamento, está em `scripts/config/fontes.py`.

**Publicação no Kaggle**

```bash
python -m scripts.kaggle.load_kaggle_datahub
```

---

## 📄 Licença e Créditos

Este dataset consolidado é disponibilizado sob licença **CC0 1.0** (domínio público). Isso se refere ao trabalho de curadoria, padronização e harmonização realizado neste repositório - os dados originais permanecem de titularidade e responsabilidade das instituições abaixo, que devem ser citadas ao utilizar cada fonte individualmente:

- **DATASUS (SIM, SINASC, CNES, SIH/SUS, SINAN):**
  > BRASIL. Ministério da Saúde. DATASUS. Brasília, DF: Ministério da Saúde. Disponível em: <https://datasus.saude.gov.br/>.

- **IBGE (População, PIB, PNS):**
  > INSTITUTO BRASILEIRO DE GEOGRAFIA E ESTATÍSTICA (IBGE). Rio de Janeiro: IBGE. Disponível em: <https://www.ibge.gov.br/>.

Se você utilizar este dataset em pesquisas, reportagens ou análises, considere citar tanto a fonte original relevante (acima) quanto este repositório de curadoria.

---

#### **Idealização e manutenção:**
- [Rafael Trindade](https://www.linkedin.com/in/rafatrindade/)