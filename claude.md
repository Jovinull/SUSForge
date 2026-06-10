# SUSForge — Documento de Governança Técnica

> Este documento é a **fonte da verdade** sobre o projeto SUSForge.
> Toda decisão arquitetural, padrão de código e convenção de infraestrutura
> deve ser refletida aqui. Agentes assistidos por IA (Claude Code, Copilot,
> etc.) DEVEM ler este arquivo antes de gerar qualquer código.

---

## 1. Visão Geral

**SUSForge** é uma plataforma de Data Warehouse analítico para dados públicos
de saúde do Brasil, alimentada pelo **OpenDATASUS** (DATASUS / Ministério da
Saúde). O projeto extrai, normaliza e modela datasets como SIM, SINAN, SIH,
SIA, CNES, IBGE territorial, entre outros, e os disponibiliza em dashboards
de alta performance para consumo analítico, epidemiológico e de gestão.

### 1.1 Objetivos

1. **Confiabilidade**: pipelines idempotentes, versionados e observáveis.
2. **Reprodutibilidade**: toda a stack roda em contêineres locais com um
   `docker compose up`.
3. **Performance**: processamento colunar com Polars; queries analíticas
   sobre modelos dimensionais (Kimball) em Postgres.
4. **Governança**: linhagem clara Bronze → Silver → Gold, com SQL e Python
   tipados, testáveis e revisáveis.
5. **Espacialidade**: suporte first-class a dados geográficos via PostGIS
   (municípios IBGE, regiões de saúde, malhas territoriais).

### 1.2 Escopo

- **Dentro do escopo**: ingestão de OpenDATASUS, modelagem dimensional,
  publicação em Metabase, orquestração via Airflow.
- **Fora do escopo (nesta fase)**: dados clínicos identificáveis, dados
  protegidos por LGPD em nível individual, integração com prontuários.

---

## 2. Arquitetura Medalhão

A arquitetura segue o padrão **Medallion (Bronze / Silver / Gold)**,
adaptado para o ecossistema OpenDATASUS.

### 2.1 Bronze — Raw / Imutável

- **Conteúdo**: dados crus do DATASUS, tal como baixados (`.dbc`, `.csv`,
  `.parquet`).
- **Camada física**: `data/raw/` (lake local, fora do Git) **e** schema
  `bronze` no Postgres, contendo metadados de ingestão (arquivo, hash,
  data de extração, fonte URL).
- **Regras**:
  - **Imutável**: nunca sobrescrever. Particionamento por
    `dataset/uf/competencia/`.
  - Conversão `.dbc → .parquet` é etapa Bronze (sem alteração semântica).
  - Toda linha carrega `_ingested_at`, `_source_file`, `_source_hash`.
  - Falhas de schema não são corrigidas aqui — são registradas.

### 2.2 Silver — Cleansed / Normalizado

- **Conteúdo**: dados limpos, tipados, com domínios decodificados (CID,
  CBO, CNES, municípios IBGE) e qualidade tratada.
- **Camada física**: schema `silver` no Postgres.
- **Regras**:
  - Tipagem estrita (datas, decimais, booleanos).
  - Decodificação de tabelas de domínio do DATASUS (ex.: `SEXO` 1/2/3 →
    enum legível).
  - Normalização de chaves: `cod_municipio_ibge` (7 dígitos), `co_cnes`,
    `cid10`, `cid_capitulo`.
  - Deduplicação e validações declarativas (regras explícitas, não mágicas).
  - Compatibilidade com PostGIS quando houver dimensão territorial.

### 2.3 Gold — Modelagem Dimensional para BI

- **Conteúdo**: star schemas Kimball — fatos e dimensões prontos para
  consumo analítico no Metabase.
- **Camada física**: schema `gold` no Postgres.
- **Regras**:
  - Nomenclatura: `fato_*`, `dim_*`, `agg_*`.
  - Chaves substitutas (`sk_*`) inteiras, geradas em ETL.
  - Dimensões conformes (ex.: `dim_municipio`, `dim_tempo`,
    `dim_estabelecimento`) compartilhadas entre fatos.
  - Granularidade declarada explicitamente em comentários da tabela.
  - Índices e particionamento pensados para queries OLAP.

### 2.4 Fluxo de Promoção

```
OpenDATASUS  ─►  data/raw/  ─►  schema bronze
                                    │
                                    ▼
                              schema silver  (limpeza + tipagem + domínios)
                                    │
                                    ▼
                              schema gold    (fatos / dimensões Kimball)
                                    │
                                    ▼
                                 Metabase
```

Cada promoção é uma DAG do Airflow; cada DAG é idempotente e
re-executável por janela temporal (competência DATASUS).

---

## 3. Stack Tecnológica

| Camada              | Tecnologia                          | Papel                                       |
|---------------------|-------------------------------------|---------------------------------------------|
| Containerização     | Docker + Docker Compose             | Reprodutibilidade local da stack            |
| Banco analítico     | PostgreSQL 16 + PostGIS             | Armazenamento das 3 camadas                 |
| Orquestração        | Apache Airflow (LocalExecutor)      | Agendamento, retries, observabilidade       |
| Processamento       | Python 3.11+ com Polars             | ETL colunar de alta performance             |
| Conversão DBC       | `pyreaddbc` / `datasus-dbc`         | `.dbc → .parquet` na camada Bronze          |
| Geoprocessamento    | PostGIS, GeoPandas (somente borda)  | Malhas IBGE, regiões de saúde               |
| BI / Visualização   | Metabase                            | Dashboards analíticos sobre o Gold          |
| Qualidade de código | ruff, mypy, pytest                  | Lint, tipagem, testes                       |

**Regra de stack**: nada de Pandas no caminho quente.  Polars é o padrão.

---

## 4. Padrões de Código Estritos

### 4.1 Python

- **Versão**: Python **3.11+**.
- **Tipagem obrigatória**: toda função pública deve ter type hints
  completos em parâmetros e retorno. `mypy --strict` é a meta.
- **Polars no lugar de Pandas**: Pandas é proibido em pipelines de
  produção. Exceção tolerada apenas em notebooks exploratórios e em
  bibliotecas terceiras que o exijam internamente.
- **Imutabilidade**: prefira `LazyFrame` e `with_columns` a mutações.
- **Estrutura**:
  - Pacote raiz: `src/susforge/` (layout *src-layout*).
  - Submódulos por camada: `bronze/`, `silver/`, `gold/`, `io/`, `domain/`.
- **Lint / format**: `ruff` (regras `E,F,I,N,UP,B,SIM,RUF`).
- **Tratamento de erros**: exceções específicas; nunca `except:` nu.
  Logs estruturados (JSON) com `structlog` ou `logging` configurado.
- **Configuração**: via `pydantic-settings` lendo `.env`. Nada de
  hardcode de credenciais.
- **Testes**: `pytest`, com fixtures para Polars e Postgres efêmero
  (`testcontainers` ou schema dedicado).

### 4.2 SQL

- **Estilo**: `snake_case` em **tudo** (tabelas, colunas, schemas,
  views, funções).
- **Palavras-chave**: SQL em **MAIÚSCULAS** (`SELECT`, `FROM`, `JOIN`).
- **DDL versionado**: todo schema vive em `sql/{bronze,silver,gold}/`
  com numeração (`001_create_dim_municipio.sql`).
- **Idempotência**: prefira `CREATE TABLE IF NOT EXISTS`,
  `CREATE OR REPLACE VIEW`. DDL destrutivo (`DROP`) só com migração
  explícita.
- **Comentários**: toda tabela e coluna do **Gold** deve ter `COMMENT ON`
  descrevendo granularidade e fonte.
- **Chaves**: PKs explícitas; FKs declaradas mesmo quando o ETL garante
  integridade.
- **Índices**: criados no mesmo arquivo da tabela; nomeados
  `ix_<tabela>_<coluna>`.

### 4.3 Airflow

- **DAGs**: uma DAG por dataset × camada (ex.: `bronze_sim_ingest`,
  `silver_sim_normalize`, `gold_fato_obitos_build`).
- **Decorator API** (`@dag`, `@task`) é o padrão. Operadores clássicos
  só quando necessário.
- **Idempotência obrigatória**: toda task deve ser re-executável sem
  efeito colateral.
- **Parâmetros**: competência (`AAAAMM`) e UF como `params` da DAG.
- **Sem lógica pesada em DAG**: a DAG orquestra; o ETL vive em
  `src/susforge/`.
- **Conexões e variáveis**: definidas via `airflow/config/` ou env
  vars, nunca hardcoded.

### 4.4 Docker

- **Imagens versionadas** (sem `:latest` em produção/composição).
- **Multi-stage builds** para Python quando aplicável.
- **Volumes nomeados** para estado persistente (Postgres, Metabase).
- **Healthchecks** em todos os serviços.
- **Networks** explícitas; sem `host` networking.

### 4.5 Git

- **Branching**: `main` protegida; trabalho em `feat/*`, `fix/*`,
  `chore/*`.
- **Commits**: Conventional Commits (`feat:`, `fix:`, `refactor:`,
  `docs:`, `chore:`).
- **Dados NUNCA são commitados** — ver `.gitignore`.

---

## 5. Estrutura de Diretórios

```
SUSForge/
├── claude.md                       # ESTE arquivo — governança do projeto
├── README.md                       # Visão executiva e quickstart
├── docker-compose.yml              # Stack completa (Postgres+PostGIS, Airflow, Metabase)
├── requirements.txt                # Dependências Python (camada de orquestração)
├── .gitignore                      # Blindagem contra commit de dados/segredos
│
├── airflow/                        # Tudo de orquestração
│   ├── config/                     # airflow.cfg, connections, variables
│   ├── plugins/                    # Operadores e hooks customizados
│   └── dags/
│       ├── bronze/                 # DAGs de ingestão crua (OpenDATASUS → raw → bronze)
│       ├── silver/                 # DAGs de limpeza e normalização
│       └── gold/                   # DAGs de construção dimensional
│
├── data/                           # Lake local — NUNCA versionado
│   ├── raw/                        # Arquivos crus do DATASUS (.dbc, .csv, .parquet)
│   ├── staging/                    # Intermediários de processamento
│   └── processed/                  # Saídas finais antes do load no Postgres
│
├── infra/                          # Infraestrutura como código
│   └── docker/
│       ├── postgres/               # Dockerfile, init scripts, extensões (PostGIS)
│       ├── airflow/                # Dockerfile customizado do Airflow
│       └── metabase/               # Configurações de inicialização do Metabase
│
├── src/                            # Código de produção (src-layout)
│   └── susforge/                   # Pacote Python principal
│       └── __init__.py
│
├── sql/                            # DDL versionado por camada
│   ├── bronze/                     # Schemas de aterrissagem
│   ├── silver/                     # Schemas normalizados
│   └── gold/                       # Star schemas (fatos e dimensões)
│
└── tests/                          # Testes unitários e de integração
```

---

## 6. Convenções de Nomenclatura

| Tipo                    | Padrão                                | Exemplo                              |
|-------------------------|---------------------------------------|--------------------------------------|
| Schema Postgres         | `snake_case`                          | `bronze`, `silver`, `gold`           |
| Tabela Bronze           | `br_<dataset>_<subset>`               | `br_sim_do`, `br_sinan_dengue`       |
| Tabela Silver           | `sv_<dataset>_<entidade>`             | `sv_sim_obitos`, `sv_cnes_estab`     |
| Tabela Gold — Fato      | `fato_<assunto>_<grão>`               | `fato_obitos_mensal`                 |
| Tabela Gold — Dimensão  | `dim_<entidade>`                      | `dim_municipio`, `dim_tempo`         |
| Chave substituta        | `sk_<entidade>`                       | `sk_municipio`                       |
| Chave natural           | `nk_<entidade>` ou nome do domínio    | `cod_municipio_ibge`                 |
| DAG Airflow             | `<camada>_<dataset>_<acao>`           | `bronze_sim_ingest`                  |
| Arquivo SQL             | `NNN_<acao>_<objeto>.sql`             | `010_create_dim_municipio.sql`       |
| Módulo Python           | `snake_case.py`                       | `dbc_to_parquet.py`                  |
| Classe Python           | `PascalCase`                          | `DatasusExtractor`                   |

---

## 7. Segurança e Conformidade

- Dados do OpenDATASUS são **públicos**, mas o tratamento segue boas
  práticas: sem PII identificável em camadas analíticas.
- Credenciais (Postgres, Metabase, Airflow) **somente** em `.env` local
  (template em `.env.example`, que pode ser versionado vazio).
- Segredos em produção (quando houver) via secret manager — nunca em
  imagem Docker, nunca em commit.

---

## 8. Regras para Agentes de IA

Ao gerar código para este projeto, agentes DEVEM:

1. Respeitar a separação Bronze / Silver / Gold — não pular camadas.
2. Usar **Polars**, não Pandas, em qualquer pipeline.
3. Tipar tudo (`mypy --strict` mindset).
4. Escrever SQL em `snake_case` com palavras-chave em maiúsculas.
5. Manter idempotência em DAGs e migrações.
6. Nunca sugerir commit de arquivos sob `data/`.
7. Antes de criar arquivo novo, verificar se a estrutura desta seção 5
   já o acomoda.
8. Preferir editar arquivos existentes a multiplicar artefatos.

---

_Última revisão: 2026-06-10 — bootstrap inicial do projeto._
