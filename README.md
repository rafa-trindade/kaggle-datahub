# Kaggle DataHub: Pipeline de Orquestração de Dados

**Pipeline de orquestração de dados (ETL) para automatizar a coleta, processamento e armazenamento de *datasets* do Kaggle, utilizando Apache Airflow e Docker.**

## 💡 Visão Geral do Projeto

O **Kaggle DataHub** é um projeto desenvolvido em Python com o objetivo de criar uma solução robusta e automatizada para a ingestão contínua de dados da plataforma Kaggle. A arquitetura é baseada em contêineres Docker para garantir um ambiente isolado e reproduzível, e utiliza o **Apache Airflow** como orquestrador principal para gerenciar o fluxo de trabalho de ETL (Extração, Transformação e Carga).

Este projeto é ideal para:
*   Engenheiros de Dados que buscam um *boilerplate* para projetos de ETL.
*   Cientistas de Dados que desejam manter seus modelos atualizados com os dados mais recentes do Kaggle.
*   Qualquer pessoa interessada em aprender sobre orquestração de dados com Airflow e Docker.

### 🛠️ Tecnologias Utilizadas

| Tecnologia | Propósito |
| :--- | :--- |
| **Python** | Linguagem principal para scripts de ETL e DAGs do Airflow. |
| **Apache Airflow** | Orquestração e agendamento do pipeline de dados. |
| **Docker & Docker Compose** | Contêinerização do ambiente e serviços. |
| **Kaggle API** | Extração programática de *datasets*. |

## 📂 Estrutura do Repositório

A estrutura de pastas reflete a organização necessária para um projeto de Airflow e Docker:

| Pasta/Arquivo | Descrição |
| :--- | :--- |
| `airflow/dags/` | Contém os arquivos de *Directed Acyclic Graph* (DAGs) do Airflow, que definem os pipelines de dados. |
| `data/` | Diretório destinado ao armazenamento dos *datasets* extraídos e processados. |
| `docker/` | Arquivos de configuração Docker, como `Dockerfile`s personalizados para os serviços. |
| `docs/` | Documentação adicional do projeto, diagramas ou guias. |
| `scripts/` | Scripts Python auxiliares para as etapas de Extração e Transformação (ETL). |
| `docker-compose.yml` | Define e executa a aplicação multi-contêiner (Airflow, Banco de Dados, etc.). |

