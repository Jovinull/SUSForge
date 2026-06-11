-- =====================================================================
-- gold.dim_municipio — Dimensão de municípios derivada das tabelas Silver
-- Granularidade: 1 linha por co_ibge
-- Origem: silver.estabelecimentos (co_ibge) + silver.leitos_anual (nome)
-- Estratégia: TRUNCATE + INSERT — recriação total
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_municipio (
    co_ibge             TEXT        PRIMARY KEY,        -- como vem da fonte (6 ou 7 dígitos)
    co_ibge_7dig        TEXT        NOT NULL,           -- normalizado para 7 dígitos
    no_municipio        TEXT,
    co_uf               TEXT,
    sg_uf               TEXT,
    ds_uf               TEXT,
    qtd_estabelecimentos INTEGER    NOT NULL DEFAULT 0,
    qtd_ubs             INTEGER     NOT NULL DEFAULT 0,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dim_municipio_uf
    ON gold.dim_municipio (sg_uf);
CREATE INDEX IF NOT EXISTS ix_dim_municipio_ibge_7
    ON gold.dim_municipio (co_ibge_7dig);

COMMENT ON TABLE  gold.dim_municipio IS
    'Municípios derivados das Silver. Inclui contagem de estabelecimentos e UBS por município.';
COMMENT ON COLUMN gold.dim_municipio.co_ibge_7dig IS
    'Código IBGE normalizado em 7 dígitos (lpad) — para JOIN futuro com dados IBGE territoriais.';

-- ---------------------------------------------------------------------
-- ELT — recarga idempotente
-- ---------------------------------------------------------------------
TRUNCATE TABLE gold.dim_municipio;

WITH uf_map(co_uf, sg_uf, ds_uf) AS (
    VALUES
        ('11','RO','Rondônia'), ('12','AC','Acre'), ('13','AM','Amazonas'),
        ('14','RR','Roraima'), ('15','PA','Pará'), ('16','AP','Amapá'),
        ('17','TO','Tocantins'), ('21','MA','Maranhão'), ('22','PI','Piauí'),
        ('23','CE','Ceará'), ('24','RN','Rio Grande do Norte'), ('25','PB','Paraíba'),
        ('26','PE','Pernambuco'), ('27','AL','Alagoas'), ('28','SE','Sergipe'),
        ('29','BA','Bahia'), ('31','MG','Minas Gerais'), ('32','ES','Espírito Santo'),
        ('33','RJ','Rio de Janeiro'), ('35','SP','São Paulo'), ('41','PR','Paraná'),
        ('42','SC','Santa Catarina'), ('43','RS','Rio Grande do Sul'),
        ('50','MS','Mato Grosso do Sul'), ('51','MT','Mato Grosso'),
        ('52','GO','Goiás'), ('53','DF','Distrito Federal')
),
-- Snapshot mais recente de estabelecimentos por município
estab_por_ibge AS (
    SELECT
        e.co_ibge,
        e.co_uf,
        count(*) AS qtd_estabelecimentos
    FROM silver.estabelecimentos e
    WHERE e._extraction_date = (SELECT MAX(_extraction_date) FROM silver.estabelecimentos)
      AND e.co_ibge IS NOT NULL
    GROUP BY e.co_ibge, e.co_uf
),
-- Nome do município via leitos (silver.estabelecimentos não tem nome)
nome_por_ibge AS (
    SELECT
        e.co_ibge,
        -- pega a forma mais frequente do nome para esse co_ibge
        mode() WITHIN GROUP (ORDER BY l.municipio) AS no_municipio
    FROM silver.estabelecimentos e
    JOIN silver.leitos_anual l
      ON l.cnes = lpad(e.co_cnes, 7, '0')
    WHERE e._extraction_date = (SELECT MAX(_extraction_date) FROM silver.estabelecimentos)
      AND l._extraction_date = (SELECT MAX(_extraction_date) FROM silver.leitos_anual)
      AND e.co_ibge IS NOT NULL
      AND l.municipio IS NOT NULL
    GROUP BY e.co_ibge
),
-- UBS por município (snapshot mais recente)
ubs_por_ibge AS (
    SELECT co_ibge, count(*) AS qtd_ubs
    FROM silver.ubs
    WHERE _extraction_date = (SELECT MAX(_extraction_date) FROM silver.ubs)
      AND co_ibge IS NOT NULL
    GROUP BY co_ibge
)
INSERT INTO gold.dim_municipio (
    co_ibge, co_ibge_7dig, no_municipio,
    co_uf, sg_uf, ds_uf,
    qtd_estabelecimentos, qtd_ubs
)
SELECT
    e.co_ibge,
    lpad(e.co_ibge, 7, '0')                 AS co_ibge_7dig,
    n.no_municipio,
    e.co_uf,
    u.sg_uf,
    u.ds_uf,
    e.qtd_estabelecimentos,
    COALESCE(ub.qtd_ubs, 0)                 AS qtd_ubs
FROM estab_por_ibge e
LEFT JOIN nome_por_ibge n ON n.co_ibge = e.co_ibge
LEFT JOIN uf_map u ON u.co_uf = e.co_uf
LEFT JOIN ubs_por_ibge ub ON ub.co_ibge = e.co_ibge;

ANALYZE gold.dim_municipio;
