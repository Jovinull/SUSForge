-- =====================================================================
-- silver.ocupacao_covid — Registro de ocupação hospitalar COVID-19
-- Granularidade: 1 linha por (_extraction_date, id_registro)
-- Origem: bronze/covid-ocupacao/leitos (e-SUS Notifica, 2020–2022)
-- Decisão: tabela única — 1.5M linhas, cabem com folga em B-tree
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.ocupacao_covid (
    id_registro                 TEXT        NOT NULL,
    cnes                        TEXT        NOT NULL,
    data_notificacao            TIMESTAMPTZ NOT NULL,
    ano_referencia              INTEGER     NOT NULL,
    -- ---- Ocupação clínica ----
    ocupacao_suspeito_cli       INTEGER     NOT NULL DEFAULT 0,
    ocupacao_confirmado_cli     INTEGER     NOT NULL DEFAULT 0,
    ocupacao_covid_cli          INTEGER     NOT NULL DEFAULT 0,
    ocupacao_hospitalar_cli     INTEGER     NOT NULL DEFAULT 0,
    -- ---- Ocupação UTI ----
    ocupacao_suspeito_uti       INTEGER     NOT NULL DEFAULT 0,
    ocupacao_confirmado_uti     INTEGER     NOT NULL DEFAULT 0,
    ocupacao_covid_uti          INTEGER     NOT NULL DEFAULT 0,
    ocupacao_hospitalar_uti     INTEGER     NOT NULL DEFAULT 0,
    -- ---- Desfechos ----
    saida_suspeita_obitos       INTEGER     NOT NULL DEFAULT 0,
    saida_suspeita_altas        INTEGER     NOT NULL DEFAULT 0,
    saida_confirmada_obitos     INTEGER     NOT NULL DEFAULT 0,
    saida_confirmada_altas      INTEGER     NOT NULL DEFAULT 0,
    -- ---- Localização ----
    estado_notificacao          TEXT,
    municipio_notificacao       TEXT,
    estado                      TEXT,
    municipio                   TEXT,
    -- ---- Metadados ----
    origem                      TEXT,
    excluido                    BOOLEAN,
    validado                    BOOLEAN,
    registro_created_at         TIMESTAMPTZ,
    registro_updated_at         TIMESTAMPTZ,
    -- ---- Linhagem ----
    _ingested_at                TIMESTAMPTZ NOT NULL,
    _source_file                TEXT        NOT NULL,
    _source_hash                TEXT        NOT NULL,
    _extraction_date            DATE        NOT NULL,
    _cleansed_at                TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (_extraction_date, id_registro)
);

CREATE INDEX IF NOT EXISTS ix_ocupacao_covid_cnes
    ON silver.ocupacao_covid (cnes);
CREATE INDEX IF NOT EXISTS ix_ocupacao_covid_ano
    ON silver.ocupacao_covid (ano_referencia);
CREATE INDEX IF NOT EXISTS ix_ocupacao_covid_data
    ON silver.ocupacao_covid (data_notificacao);
CREATE INDEX IF NOT EXISTS ix_ocupacao_covid_estado
    ON silver.ocupacao_covid (estado, data_notificacao);
CREATE INDEX IF NOT EXISTS ix_ocupacao_covid_extr
    ON silver.ocupacao_covid (_extraction_date);

COMMENT ON TABLE  silver.ocupacao_covid IS
    'Registros diários de ocupação hospitalar COVID-19 (e-SUS Notifica). Granularidade: notificação individual. JOIN cnes ⇄ silver.estabelecimentos.';
COMMENT ON COLUMN silver.ocupacao_covid.id_registro IS
    'UUID do registro no e-SUS Notifica (Parse).';
COMMENT ON COLUMN silver.ocupacao_covid.cnes IS
    '7 dígitos zero-padded — chave estrangeira lógica para silver.estabelecimentos.';
COMMENT ON COLUMN silver.ocupacao_covid.ocupacao_covid_uti IS
    'Leitos UTI ocupados por pacientes COVID confirmados.';
COMMENT ON COLUMN silver.ocupacao_covid.ocupacao_hospitalar_uti IS
    'Total de leitos UTI ocupados (todos os pacientes, não só COVID).';
