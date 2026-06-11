-- =====================================================================
-- silver.leitos_anual — Oferta de leitos hospitalares por estabelecimento × competência
-- Granularidade: 1 linha por (_extraction_date, comp, cnes)
-- Origem: bronze/hospitais-leitos/leitos (via Polars + Pandera; consolida 20 anos)
-- Decisão: tabela única (não particionada) — 1.4M linhas cabem com folga
--          em índice B-tree; particionamento por ano não traria ganho
--          mensurável e adicionaria complexidade de manutenção.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.leitos_anual (
    -- ---- Tempo ----
    ano_referencia          INTEGER     NOT NULL,
    comp                    TEXT        NOT NULL,           -- "YYYYMM"
    -- ---- Estabelecimento ----
    cnes                    TEXT        NOT NULL,
    nome_estabelecimento    TEXT,
    razao_social            TEXT,
    motivo_desabilitacao    TEXT,
    tp_gestao               TEXT,
    co_tipo_unidade         TEXT,
    ds_tipo_unidade         TEXT,
    natureza_juridica       TEXT,
    desc_natureza_juridica  TEXT,
    -- ---- Localização ----
    regiao                  TEXT,
    uf                      TEXT,
    municipio               TEXT,
    -- ---- Endereço ----
    no_logradouro           TEXT,
    nu_endereco             TEXT,
    no_complemento          TEXT,
    no_bairro               TEXT,
    co_cep                  TEXT,
    nu_telefone             TEXT,
    no_email                TEXT,
    -- ---- Quantitativos (FATO — INT NOT NULL, default 0) ----
    leitos_existentes               INTEGER     NOT NULL DEFAULT 0,
    leitos_sus                      INTEGER     NOT NULL DEFAULT 0,
    uti_total_exist                 INTEGER     NOT NULL DEFAULT 0,
    uti_total_sus                   INTEGER     NOT NULL DEFAULT 0,
    uti_adulto_exist                INTEGER     NOT NULL DEFAULT 0,
    uti_adulto_sus                  INTEGER     NOT NULL DEFAULT 0,
    uti_pediatrico_exist            INTEGER     NOT NULL DEFAULT 0,
    uti_pediatrico_sus              INTEGER     NOT NULL DEFAULT 0,
    uti_neonatal_exist              INTEGER     NOT NULL DEFAULT 0,
    uti_neonatal_sus                INTEGER     NOT NULL DEFAULT 0,
    uti_queimado_exist              INTEGER     NOT NULL DEFAULT 0,
    uti_queimado_sus                INTEGER     NOT NULL DEFAULT 0,
    uti_coronariana_exist           INTEGER     NOT NULL DEFAULT 0,
    uti_coronariana_sus             INTEGER     NOT NULL DEFAULT 0,
    -- ---- Linhagem ----
    _ingested_at            TIMESTAMPTZ NOT NULL,
    _source_file            TEXT        NOT NULL,
    _source_hash            TEXT        NOT NULL,
    _extraction_date        DATE        NOT NULL,
    _cleansed_at            TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (_extraction_date, comp, cnes)
);

CREATE INDEX IF NOT EXISTS ix_leitos_anual_cnes
    ON silver.leitos_anual (cnes);
CREATE INDEX IF NOT EXISTS ix_leitos_anual_ano
    ON silver.leitos_anual (ano_referencia);
CREATE INDEX IF NOT EXISTS ix_leitos_anual_uf_ano
    ON silver.leitos_anual (uf, ano_referencia);
CREATE INDEX IF NOT EXISTS ix_leitos_anual_extr
    ON silver.leitos_anual (_extraction_date);

COMMENT ON TABLE  silver.leitos_anual IS
    'Oferta mensal de leitos hospitalares por estabelecimento. Consolida 20 anos da série CNES/Leitos. Granularidade: (extracao, comp YYYYMM, cnes).';
COMMENT ON COLUMN silver.leitos_anual.comp IS
    'Competência mensal no formato "YYYYMM".';
COMMENT ON COLUMN silver.leitos_anual.ano_referencia IS
    'Ano derivado da competência — útil para queries analíticas anuais.';
COMMENT ON COLUMN silver.leitos_anual.cnes IS
    'Código CNES — chave estrangeira lógica para silver.estabelecimentos.';
COMMENT ON COLUMN silver.leitos_anual.leitos_existentes IS
    'Leitos hospitalares totais existentes no estabelecimento (todas as origens).';
COMMENT ON COLUMN silver.leitos_anual.leitos_sus IS
    'Subconjunto de leitos_existentes disponibilizados ao SUS.';
