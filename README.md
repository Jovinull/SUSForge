# SUSForge

**Data Warehouse Medalhão para dados públicos de saúde do Brasil (OpenDATASUS / CKAN-MS).**
Extração automatizada, modelagem dimensional (Kimball), validação por contrato e BI auto-publicado — tudo orquestrado por Airflow 3 em contêineres.

---

## Visão geral

O SUSForge transforma datasets brutos do Ministério da Saúde (CNES, Leitos hospitalares, Ocupação COVID-19, UBS) em um Data Warehouse analítico pronto para BI. A arquitetura Medalhão (Bronze → Silver → Gold) garante governança, imutabilidade e linhagem em todas as camadas.

O projeto cobre o ciclo completo de engenharia de dados moderna:
- **Ingestão multi-formato** (S3 público, CSV anual paginado por ano, CSV direto sem ZIP, API REST paginada)
- **Validação por contrato** com Pandera + Polars (tipos estritos, envelope geográfico, sentinelas)
- **Modelagem dimensional Kimball** (Star Schema, dim conformes, fatos derivadas)
- **Habilitação espacial** com PostGIS (`GEOGRAPHY(Point, 4326)` + GiST)
- **BI customizado** em **Next.js 15 (App Router)** com Server Components consultando direto a Camada Gold via `pg` Pool

---

## Stack

| Camada | Tecnologia |
|---|---|
| Orquestração | **Apache Airflow 3.2** (TaskFlow API, LocalExecutor, FAB auth manager) |
| Banco analítico | **PostgreSQL 16** + **PostGIS 3.4** |
| Processamento | **Polars 1.x** (Pandas é proibido no caminho quente) |
| Validação | **Pandera 0.31** (suporte nativo a Polars) |
| Loader Postgres | **psycopg2 COPY** in-memory (~57 k linhas/s) |
| Containerização | **Docker Compose** com healthchecks e volumes nomeados |
| BI | **Next.js 15 + React 19** (App Router + Server Components) · **Tailwind CSS** · **Recharts** · **lucide-react** |
| Conexão DB do frontend | **node-postgres** (`pg.Pool`) singleton |
| Qualidade | **ruff** + **mypy --strict** + **pytest** + **pre-commit** |
| Configuração | **pydantic-settings** 2.x (BaseSettings tipado) |

---

## Arquitetura

```
                  ┌────────────────────┐
                  │  Fontes Públicas   │
                  │  CKAN-MS · DEMAS   │
                  └─────────┬──────────┘
                            │
            ┌───────────────┴───────────────┐
            │      4 Moldes de Extração     │
            │  (Bronze — imutável,          │
            │   linhagem completa)          │
            └───────────────┬───────────────┘
                            │
                  ┌─────────▼──────────┐
                  │  Silver — Postgres │
                  │  Pandera contratos │
                  │  Limpeza + tipagem │
                  └─────────┬──────────┘
                            │
                  ┌─────────▼──────────┐
                  │  Gold — Star Schema│
                  │  Dim conformes +   │
                  │  Fatos derivadas   │
                  └─────────┬──────────┘
                            │
                  ┌─────────▼──────────┐
                  │  Frontend Next.js  │
                  │  Server Components │
                  │  Tailwind · Recharts│
                  └────────────────────┘
```

### Os 4 moldes de extração

O projeto provou na prática 4 padrões distintos de ingestão Bronze, todos com **manifest JSON atômico**, **detecção de mudança** e **idempotência absoluta**:

| # | Molde | Dataset exemplo | Detecção de mudança |
|---|---|---|---|
| 1 | **ZIP único do S3 público** | CNES (622k estabelecimentos) | HTTP `HEAD` → ETag |
| 2 | **ZIP por ano (multi-arquivo)** | Hospitais e Leitos (20 anos, 1,6M linhas) | ETag por ano + fallback offline por ano |
| 3 | **CSV direto multi-ano** | Ocupação COVID-19 (3 anos, 1,6M linhas) | ETag por ano + URL volátil controlada |
| 4 | **API REST paginada** | Vacinação PNI/DEMAS | Hash determinístico do conjunto de UUIDs |

Todos os extratores tratam falhas de rede via **fallback offline** (cópia local em `docs/`), com hash check para preservar imutabilidade.

---

## Volumes transacionados

### Silver — dados limpos e tipados

| Tabela | Linhas | Conteúdo |
|---|---:|---|
| `silver.estabelecimentos` | 621.874 | CNES Master Data (39 colunas tipadas) |
| `silver.leitos_anual` | 1.611.796 | 20 anos de capacidade hospitalar mensal |
| `silver.ocupacao_covid` | 1.591.625 | Notificações diárias de ocupação COVID (2020–2022) |
| `silver.ubs` | 47.768 | Atenção Primária — Unidades Básicas de Saúde |
| `silver.dominio_tp_unidade` | 38 | Decodificação curada de tipos de unidade |
| `silver.dominio_natureza_juridica` | 31 | Decodificação curada de natureza jurídica (CONCLA) |

### Gold — Star Schema pronto para BI

| Tabela | Linhas | Papel |
|---|---:|---|
| `gold.dim_estabelecimento` | 621.874 | Dimensão master · `is_ubs` · `geom` PostGIS · 99,9% decodificada |
| `gold.dim_tempo` | 8.766 | Dimensão temporal diária (2007 → 2030) |
| `gold.dim_municipio` | 5.585 | Municípios derivados das Silver |
| `gold.fato_leitos_anual` | 146.970 | Capacidade hospitalar (cnes × ano) |
| `gold.fato_ocupacao_covid` | 70.149 | Ocupação mensal COVID (cnes × ano_mes) |
| `gold.fato_pressao_uti_covid` | 62.067 | **View de ouro**: capacidade vs uso real |

**Total: ~4,5 milhões de linhas Silver + 12 tabelas analíticas Gold.**

---

## A "View de Ouro" — Pressão UTI COVID

Materializa o cruzamento que motiva todo o DW:

```sql
SELECT d.no_fantasia, d.sg_uf, p.ano_mes,
       p.capacidade_uti_declarada,
       p.pico_uti_covid_mes,
       p.taxa_ocupacao_uti_covid,    -- > 1.0 = UTI improvisada
       p.taxa_letalidade
FROM gold.fato_pressao_uti_covid p
JOIN gold.dim_estabelecimento d ON d.co_cnes = p.co_cnes
WHERE p.is_overloaded
ORDER BY p.taxa_ocupacao_uti_covid DESC;
```

**903 registros (hospital × mês) com `is_overloaded = TRUE`** — hospitais que transformaram alas inteiras em UTI emergencial durante a pandemia, com **taxas reais de até 1402%** da capacidade declarada e letalidade observada de até 54%.

---

## Decisões arquiteturais relevantes

- **Padrão Ouro Bronze**: particionamento por data de extração (`<YYYY-MM-DD>/`), manifest com escrita atômica (`.tmp` + rename), hash da fonte calculado **antes da cópia** quando viável, partição existente **nunca sobrescrita ou deletada**.
- **Idempotência Silver**: `replace_partition` (DELETE + COPY em transação atômica) — rerun na mesma `_extraction_date` resulta no mesmo estado.
- **Idempotência Gold**: ELT puro (`TRUNCATE` + `INSERT ... SELECT`) executado dentro do Postgres.
- **Sem FK físicas entre Gold tables** — convenção `lpad(co_cnes, 7, '0')` + índices, comum em DW analíticos.
- **Detector de drift**: o extrator de Hospitais e Leitos detecta automaticamente mudança de separador CSV (`,` → `;`) que o MS aplicou silenciosamente em 2025.

---

## BI customizado em Next.js

A camada de apresentação é um aplicativo **Next.js 15 com App Router** rodando em contêiner próprio.

- **Server Components** consultam o Postgres direto em cada request via `pg.Pool` singleton (sem REST intermediária)
- **Sem tela de login** — acesso aberto na rede local; autenticação é responsabilidade do reverse-proxy em produção
- **Dark mode default** com Tailwind CSS; visual "Enterprise BI"
- **Recharts** para gráficos interativos; **lucide-react** para ícones
- Hot reload em dev mode via bind mount do código + volumes nomeados para `node_modules` e `.next`
- Endpoint `/api/health` valida conectividade com Postgres

Estrutura do frontend:

```
frontend/
├── app/
│   ├── api/health/route.ts     # healthcheck (ping ao Postgres)
│   ├── components/             # Sidebar, KpiCard, LeitosChart, TopHospitais...
│   ├── globals.css             # Tailwind + gradiente de fundo
│   ├── layout.tsx              # RootLayout (dark mode default)
│   └── page.tsx                # Dashboard executivo (Server Component)
├── lib/
│   ├── db.ts                   # pg.Pool singleton
│   ├── queries.ts              # SQL contra gold.fato_* e gold.dim_*
│   └── format.ts               # Intl.NumberFormat pt-BR
├── package.json
├── next.config.js
├── tailwind.config.ts
└── tsconfig.json
```

Dashboard pronto em `http://localhost:3000` após `docker compose up -d` (sem qualquer setup ou login).

---

## Como rodar localmente

```bash
git clone https://github.com/Jovinull/SUSForge.git
cd SUSForge
cp .env.example .env

# Sobe todo o stack (Postgres+PostGIS, Airflow 3, Frontend Next.js)
docker compose up -d

# Acessar:
# - Frontend BI: http://localhost:3000  (sem login)
# - Airflow:     http://localhost:8080  (admin / admin_change_me)
```

Trigger das DAGs Bronze pela UI do Airflow, ou via CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger bronze_cnes_estabelecimentos
docker compose exec airflow-scheduler airflow dags trigger bronze_hospitais_leitos
docker compose exec airflow-scheduler airflow dags trigger bronze_covid_ocupacao
docker compose exec airflow-scheduler airflow dags trigger bronze_ubs
docker compose exec airflow-scheduler airflow dags trigger bronze_vacinacao_covid
```

---

## Saúde do código

- `ruff check` — 0 warnings em todo o projeto
- `mypy --strict` — 0 erros em **29 source files** (`src/` + `tests/`)
- `docker compose config` — valida
- **30 commits** no histórico, contando toda a evolução desde o bootstrap zero

---

## Estrutura do repositório

```
SUSForge/
├── airflow/dags/             # 12 DAGs (Bronze, Silver, Gold)
│   ├── bronze/               # 5 extratores (4 moldes de ingestão)
│   ├── silver/               # Limpeza + Pandera + COPY
│   └── gold/                 # ELT in-database (Star Schema)
├── infra/docker/             # Dockerfiles e init scripts
├── sql/                      # DDLs versionados por camada
│   ├── silver/               # Tabelas + domínios curados
│   └── gold/                 # Star Schema + Pressão COVID
├── src/susforge/             # Pacote Python (src-layout)
│   ├── config/               # pydantic-settings
│   ├── io/                   # Extratores Bronze + cliente DEMAS
│   ├── schemas/              # Contratos Pandera
│   ├── transformers/         # Bronze → Silver
│   ├── loaders/              # COPY de alta performance
│   └── utils/                # Utilitários auxiliares
├── docs/assistencia-saude/   # Fallback offline (CSVs + dicionários)
├── data/                     # Lake local (NUNCA versionado)
├── docker-compose.yml        # Stack completa (Postgres+PostGIS, Airflow 3, Next.js)
├── frontend/                 # BI customizado Next.js 15 (App Router)
├── infra/docker/frontend/    # Dockerfile do Next.js (node:20-alpine)
├── pyproject.toml            # ruff + mypy strict + pytest
└── claude.md                 # Governança técnica do projeto
```

---

## Licença

Proprietary.
