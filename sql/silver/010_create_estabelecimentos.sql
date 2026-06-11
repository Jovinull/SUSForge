-- =====================================================================
-- silver.estabelecimentos — CNES limpo e tipado (Master Data)
-- Granularidade: 1 linha por (extracao, co_cnes)
-- Origem: bronze/cnes/estabelecimentos (via Polars + Pandera)
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.estabelecimentos (
    co_cnes                     TEXT        NOT NULL,
    co_unidade                  TEXT,
    co_uf                       TEXT,
    co_ibge                     TEXT,
    nu_cnpj_mantenedora         TEXT,
    no_razao_social             TEXT,
    no_fantasia                 TEXT,
    co_natureza_organizacao     TEXT,
    ds_natureza_organizacao     TEXT,
    tp_gestao                   TEXT,
    co_nivel_hierarquia         TEXT,
    ds_nivel_hierarquia         TEXT,
    co_esfera_administrativa    TEXT,
    ds_esfera_administrativa    TEXT,
    co_atividade                TEXT,
    tp_unidade                  TEXT,
    co_cep                      TEXT,
    no_logradouro               TEXT,
    nu_endereco                 TEXT,
    no_bairro                   TEXT,
    nu_telefone                 TEXT,
    nu_latitude                 DOUBLE PRECISION,
    nu_longitude                DOUBLE PRECISION,
    co_turno_atendimento        TEXT,
    ds_turno_atendimento        TEXT,
    nu_cnpj                     TEXT,
    no_email                    TEXT,
    co_natureza_jur             TEXT,
    st_centro_cirurgico         BOOLEAN,
    st_centro_obstetrico        BOOLEAN,
    st_centro_neonatal          BOOLEAN,
    st_atend_hospitalar         BOOLEAN,
    st_servico_apoio            BOOLEAN,
    st_atend_ambulatorial       BOOLEAN,
    co_motivo_desab             TEXT,
    co_ambulatorial_sus         TEXT,
    -- Linhagem técnica (Bronze → Silver)
    _ingested_at                TIMESTAMPTZ NOT NULL,
    _source_file                TEXT        NOT NULL,
    _source_hash                TEXT        NOT NULL,
    _extraction_date            DATE        NOT NULL,
    _cleansed_at                TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (_extraction_date, co_cnes)
);

CREATE INDEX IF NOT EXISTS ix_estabelecimentos_co_cnes
    ON silver.estabelecimentos (co_cnes);
CREATE INDEX IF NOT EXISTS ix_estabelecimentos_co_ibge
    ON silver.estabelecimentos (co_ibge);
CREATE INDEX IF NOT EXISTS ix_estabelecimentos_co_uf
    ON silver.estabelecimentos (co_uf);
CREATE INDEX IF NOT EXISTS ix_estabelecimentos_extr
    ON silver.estabelecimentos (_extraction_date);

COMMENT ON TABLE  silver.estabelecimentos IS
    'CNES limpo e tipado — 1 linha por (extracao, co_cnes). Master Data.';
COMMENT ON COLUMN silver.estabelecimentos.co_cnes IS
    'Código CNES — chave natural do estabelecimento.';
COMMENT ON COLUMN silver.estabelecimentos.co_ibge IS
    'Código IBGE do município (6 ou 7 dígitos, conforme origem).';
COMMENT ON COLUMN silver.estabelecimentos.nu_latitude IS
    'Latitude WGS84. NULL quando ausente, zero ou fora do envelope Brasil.';
COMMENT ON COLUMN silver.estabelecimentos.nu_longitude IS
    'Longitude WGS84. NULL quando ausente, zero ou fora do envelope Brasil.';
COMMENT ON COLUMN silver.estabelecimentos._extraction_date IS
    'Data da partição de extração Bronze (compõe a PK).';
COMMENT ON COLUMN silver.estabelecimentos._cleansed_at IS
    'Timestamp UTC da limpeza/validação Silver.';
