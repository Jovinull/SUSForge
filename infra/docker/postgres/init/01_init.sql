-- =====================================================================
-- SUSForge — Inicialização do PostgreSQL
-- Executado UMA ÚNICA VEZ pelo entrypoint do Postgres, no primeiro boot
-- (quando /var/lib/postgresql/data está vazio). Rodando como o
-- superusuário ${POSTGRES_USER} definido em .env.
--
-- Responsabilidades:
--   1. Criar bancos auxiliares (Airflow e Metabase).
--   2. Criar schemas da arquitetura Medalhão no banco analítico.
--   3. Habilitar PostGIS no banco analítico (geometria, geografia,
--      topologia — usado em dim_municipio, regiões de saúde, etc.).
--
-- Idempotência: scripts em /docker-entrypoint-initdb.d só rodam quando
-- o volume está vazio, então `IF NOT EXISTS` é cinto-e-suspensório.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Bancos auxiliares para os serviços de plataforma
-- ---------------------------------------------------------------------
-- Airflow guarda DAG runs, XComs, conexões e estado do scheduler aqui.
SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

-- Metabase guarda dashboards, perguntas, usuários e configurações aqui
-- (substitui o H2 embutido, que não é adequado para produção).
SELECT 'CREATE DATABASE metabase_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase_db')\gexec

-- ---------------------------------------------------------------------
-- 2. Banco analítico — schemas Medalhão + PostGIS
-- ---------------------------------------------------------------------
-- O banco principal (POSTGRES_DB) já foi criado pelo entrypoint do
-- Postgres. Conectamos nele para montar a estrutura analítica.
\connect susforge

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA bronze IS
    'Camada Bronze — dados crus do OpenDATASUS, imutáveis, com metadados de ingestão.';
COMMENT ON SCHEMA silver IS
    'Camada Silver — dados limpos, tipados e com domínios decodificados.';
COMMENT ON SCHEMA gold  IS
    'Camada Gold — modelagem dimensional Kimball (fatos e dimensões) para consumo BI.';

-- PostGIS é first-class no SUSForge — dim_municipio, regiões de saúde,
-- malhas IBGE, etc. Habilitamos no banco analítico apenas.
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Search path padrão prioriza as camadas Medalhão (sem expor public).
ALTER DATABASE susforge SET search_path TO gold, silver, bronze, public;
