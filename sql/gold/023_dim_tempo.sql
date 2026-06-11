-- =====================================================================
-- gold.dim_tempo — Dimensão temporal canônica (granularidade diária)
-- Período: 2007-01-01 → 2030-12-31 (≈ 8.766 linhas)
-- Estratégia: TRUNCATE + INSERT via generate_series
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_tempo (
    data                DATE        PRIMARY KEY,
    ano                 INTEGER     NOT NULL,
    mes                 INTEGER     NOT NULL,           -- 1..12
    dia                 INTEGER     NOT NULL,           -- 1..31
    trimestre           INTEGER     NOT NULL,           -- 1..4
    semestre            INTEGER     NOT NULL,           -- 1..2
    ano_mes             TEXT        NOT NULL,           -- "YYYYMM" (≡ comp)
    ano_mes_label       TEXT        NOT NULL,           -- "YYYY-MM"
    nome_mes            TEXT        NOT NULL,           -- "Janeiro"…
    nome_mes_abrev      TEXT        NOT NULL,           -- "Jan"…
    dia_semana          INTEGER     NOT NULL,           -- 1=Seg, 7=Dom (ISO)
    nome_dia_semana     TEXT        NOT NULL,
    eh_fim_de_semana    BOOLEAN     NOT NULL,
    semana_iso          INTEGER     NOT NULL,
    dia_do_ano          INTEGER     NOT NULL,           -- 1..366
    -- Marcos temporais úteis no domínio saúde
    eh_periodo_covid    BOOLEAN     GENERATED ALWAYS AS (
                            data BETWEEN DATE '2020-03-01' AND DATE '2022-12-31'
                        ) STORED,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dim_tempo_ano       ON gold.dim_tempo (ano);
CREATE INDEX IF NOT EXISTS ix_dim_tempo_ano_mes   ON gold.dim_tempo (ano_mes);
CREATE INDEX IF NOT EXISTS ix_dim_tempo_trim      ON gold.dim_tempo (ano, trimestre);

COMMENT ON TABLE  gold.dim_tempo IS
    'Dimensão temporal canônica (diária). Permite drill-down ano/trim/mês/sem no Metabase e JOIN com qualquer fato via data ou ano_mes.';
COMMENT ON COLUMN gold.dim_tempo.ano_mes IS
    'Competência mensal "YYYYMM" — formato compartilhado com fato_leitos_anual.comp e fato_ocupacao_covid.ano_mes.';
COMMENT ON COLUMN gold.dim_tempo.eh_periodo_covid IS
    'Coluna gerada — TRUE para datas dentro do período de notificação COVID (mar/2020 a dez/2022).';

-- ---------------------------------------------------------------------
-- ELT — recarga idempotente
-- ---------------------------------------------------------------------
TRUNCATE TABLE gold.dim_tempo;

INSERT INTO gold.dim_tempo (
    data, ano, mes, dia, trimestre, semestre,
    ano_mes, ano_mes_label, nome_mes, nome_mes_abrev,
    dia_semana, nome_dia_semana, eh_fim_de_semana,
    semana_iso, dia_do_ano
)
SELECT
    d::date                                     AS data,
    EXTRACT(YEAR FROM d)::int                   AS ano,
    EXTRACT(MONTH FROM d)::int                  AS mes,
    EXTRACT(DAY FROM d)::int                    AS dia,
    EXTRACT(QUARTER FROM d)::int                AS trimestre,
    CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 1 ELSE 2 END AS semestre,
    to_char(d, 'YYYYMM')                        AS ano_mes,
    to_char(d, 'YYYY-MM')                       AS ano_mes_label,
    initcap(to_char(d, 'TMMonth'))              AS nome_mes,
    initcap(to_char(d, 'TMMon'))                AS nome_mes_abrev,
    EXTRACT(ISODOW FROM d)::int                 AS dia_semana,
    initcap(to_char(d, 'TMDay'))                AS nome_dia_semana,
    EXTRACT(ISODOW FROM d) IN (6, 7)            AS eh_fim_de_semana,
    EXTRACT(WEEK FROM d)::int                   AS semana_iso,
    EXTRACT(DOY FROM d)::int                    AS dia_do_ano
FROM generate_series(
    DATE '2007-01-01',
    DATE '2030-12-31',
    INTERVAL '1 day'
) AS d;

ANALYZE gold.dim_tempo;
