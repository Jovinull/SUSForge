-- =====================================================================
-- silver.ubs — Unidades Básicas de Saúde (Atenção Primária)
-- Granularidade: 1 linha por (_extraction_date, cnes)
-- Origem: bronze/ubs/unidades
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.ubs (
    cnes                TEXT        NOT NULL,
    co_uf               TEXT,
    co_ibge             TEXT,
    no_unidade          TEXT,
    no_logradouro       TEXT,
    no_bairro           TEXT,
    nu_latitude         DOUBLE PRECISION,
    nu_longitude        DOUBLE PRECISION,
    _ingested_at        TIMESTAMPTZ NOT NULL,
    _source_file        TEXT        NOT NULL,
    _source_hash        TEXT        NOT NULL,
    _extraction_date    DATE        NOT NULL,
    _cleansed_at        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (_extraction_date, cnes)
);

CREATE INDEX IF NOT EXISTS ix_ubs_cnes  ON silver.ubs (cnes);
CREATE INDEX IF NOT EXISTS ix_ubs_uf    ON silver.ubs (co_uf);
CREATE INDEX IF NOT EXISTS ix_ubs_ibge  ON silver.ubs (co_ibge);

COMMENT ON TABLE  silver.ubs IS
    'Unidades Básicas de Saúde — subset de Atenção Primária do CNES. JOIN cnes ⇄ silver.estabelecimentos.';
COMMENT ON COLUMN silver.ubs.cnes IS
    '7 dígitos zero-padded — chave estrangeira lógica para silver.estabelecimentos.';
