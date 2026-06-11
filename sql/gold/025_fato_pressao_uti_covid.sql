-- =====================================================================
-- gold.fato_pressao_uti_covid — KPI de pressão sobre o sistema durante COVID
-- Granularidade: (co_cnes, ano_mes)
-- Origem: gold.fato_leitos_anual ⨝ gold.fato_ocupacao_covid
-- Estratégia: TRUNCATE + INSERT — recriação total
--
-- Métricas-chave:
--   * taxa_ocupacao_uti_covid : pico_uti_covid_mes / capacidade_uti_declarada
--   * is_overloaded            : taxa > 1.0 (UTIs improvisadas)
--   * taxa_letalidade          : obitos / (obitos + altas)
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.fato_pressao_uti_covid (
    co_cnes                     TEXT        NOT NULL,
    ano_mes                     TEXT        NOT NULL,
    ano_referencia              INTEGER     NOT NULL,
    -- ---- Capacidade declarada (CNES/Leitos) ----
    capacidade_uti_declarada    INTEGER     NOT NULL,
    leitos_total_declarado      INTEGER     NOT NULL,
    -- ---- Pressão observada (e-SUS Notifica) ----
    pico_uti_covid_mes          INTEGER     NOT NULL,
    pico_cli_covid_mes          INTEGER     NOT NULL,
    pico_uti_total_mes          INTEGER     NOT NULL,         -- não-COVID inclusive
    qtd_notificacoes            INTEGER     NOT NULL,
    -- ---- KPIs de pressão ----
    taxa_ocupacao_uti_covid     NUMERIC(8,4),                  -- pico / capacidade
    is_overloaded               BOOLEAN     NOT NULL,
    -- ---- Desfechos ----
    obitos_confirmados          BIGINT      NOT NULL,
    altas_confirmadas           BIGINT      NOT NULL,
    taxa_letalidade             NUMERIC(8,4),                  -- obitos / (obitos+altas)
    -- ---- Linhagem ----
    _loaded_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (co_cnes, ano_mes)
);

CREATE INDEX IF NOT EXISTS ix_fato_pressao_ano_mes
    ON gold.fato_pressao_uti_covid (ano_mes);
CREATE INDEX IF NOT EXISTS ix_fato_pressao_cnes
    ON gold.fato_pressao_uti_covid (co_cnes);
CREATE INDEX IF NOT EXISTS ix_fato_pressao_overload
    ON gold.fato_pressao_uti_covid (is_overloaded)
    WHERE is_overloaded;
CREATE INDEX IF NOT EXISTS ix_fato_pressao_letalidade
    ON gold.fato_pressao_uti_covid (taxa_letalidade);

COMMENT ON TABLE  gold.fato_pressao_uti_covid IS
    'Fato derivada: pressão sobre o sistema hospitalar durante a pandemia. Cruza capacidade declarada (Leitos) com ocupação observada (COVID).';
COMMENT ON COLUMN gold.fato_pressao_uti_covid.taxa_ocupacao_uti_covid IS
    'Razão pico_uti_covid_mes / capacidade_uti_declarada. Valor > 1 indica UTI improvisada (leitos comuns convertidos em UTI emergencial).';
COMMENT ON COLUMN gold.fato_pressao_uti_covid.is_overloaded IS
    'TRUE quando o pico mensal de UTI COVID excedeu a capacidade UTI declarada no CNES.';
COMMENT ON COLUMN gold.fato_pressao_uti_covid.taxa_letalidade IS
    'obitos_confirmados / (obitos_confirmados + altas_confirmadas) — letalidade observada.';

-- ---------------------------------------------------------------------
-- ELT — recarga idempotente
-- ---------------------------------------------------------------------
TRUNCATE TABLE gold.fato_pressao_uti_covid;

INSERT INTO gold.fato_pressao_uti_covid (
    co_cnes, ano_mes, ano_referencia,
    capacidade_uti_declarada, leitos_total_declarado,
    pico_uti_covid_mes, pico_cli_covid_mes, pico_uti_total_mes,
    qtd_notificacoes,
    taxa_ocupacao_uti_covid, is_overloaded,
    obitos_confirmados, altas_confirmadas, taxa_letalidade
)
SELECT
    fc.co_cnes,
    fc.ano_mes,
    fc.ano_referencia,
    fl.uti_total_max                                AS capacidade_uti_declarada,
    fl.leitos_existentes_max                        AS leitos_total_declarado,
    fc.ocupacao_uti_covid_max                       AS pico_uti_covid_mes,
    fc.ocupacao_cli_covid_max                       AS pico_cli_covid_mes,
    fc.ocupacao_uti_hosp_max                        AS pico_uti_total_mes,
    fc.qtd_notificacoes,
    CASE
        WHEN fl.uti_total_max > 0
        THEN round((fc.ocupacao_uti_covid_max::numeric / fl.uti_total_max), 4)
        ELSE NULL
    END                                             AS taxa_ocupacao_uti_covid,
    COALESCE(
        fl.uti_total_max > 0
        AND fc.ocupacao_uti_covid_max > fl.uti_total_max,
        FALSE
    )                                               AS is_overloaded,
    fc.obitos_confirmados_sum                       AS obitos_confirmados,
    fc.altas_confirmadas_sum                        AS altas_confirmadas,
    CASE
        WHEN (fc.obitos_confirmados_sum + fc.altas_confirmadas_sum) > 0
        THEN round(
            (fc.obitos_confirmados_sum::numeric
             / (fc.obitos_confirmados_sum + fc.altas_confirmadas_sum)),
            4
        )
        ELSE NULL
    END                                             AS taxa_letalidade
FROM gold.fato_ocupacao_covid fc
JOIN gold.fato_leitos_anual fl
    ON fl.co_cnes = fc.co_cnes
   AND fl.ano_referencia = fc.ano_referencia
WHERE fc.ano_referencia BETWEEN 2020 AND 2022;

ANALYZE gold.fato_pressao_uti_covid;
