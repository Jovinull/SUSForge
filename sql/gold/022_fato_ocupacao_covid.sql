-- =====================================================================
-- gold.fato_ocupacao_covid — Ocupação COVID agregada por estabelecimento × mês
-- Granularidade: (co_cnes, ano_mes)
-- Origem: silver.ocupacao_covid (snapshot mais recente por _extraction_date)
-- Estratégia: TRUNCATE + INSERT — recriação total
--
-- Decisões analíticas:
--   * ano_mes "YYYYMM" — alinhado com gold.fato_leitos_anual.comp
--   * SUM/AVG/MAX por métrica de ocupação UTI e Clínica COVID
--   * SUMs de desfechos (óbitos + altas confirmados)
--   * Sem FK física — JOIN co_cnes ⇄ gold.dim_estabelecimento por convenção
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.fato_ocupacao_covid (
    co_cnes                         TEXT        NOT NULL,
    ano_mes                         TEXT        NOT NULL,           -- "YYYYMM"
    ano_referencia                  INTEGER     NOT NULL,
    qtd_notificacoes                INTEGER     NOT NULL,
    -- ---- Ocupação UTI COVID ----
    ocupacao_uti_covid_sum          BIGINT      NOT NULL,
    ocupacao_uti_covid_avg          NUMERIC(10,2) NOT NULL,
    ocupacao_uti_covid_max          INTEGER     NOT NULL,
    -- ---- Ocupação clínica COVID ----
    ocupacao_cli_covid_sum          BIGINT      NOT NULL,
    ocupacao_cli_covid_avg          NUMERIC(10,2) NOT NULL,
    ocupacao_cli_covid_max          INTEGER     NOT NULL,
    -- ---- Ocupação hospitalar TOTAL (referência) ----
    ocupacao_uti_hosp_max           INTEGER     NOT NULL,
    ocupacao_cli_hosp_max           INTEGER     NOT NULL,
    -- ---- Desfechos ----
    obitos_confirmados_sum          BIGINT      NOT NULL,
    altas_confirmadas_sum           BIGINT      NOT NULL,
    obitos_suspeitos_sum            BIGINT      NOT NULL,
    altas_suspeitas_sum             BIGINT      NOT NULL,
    -- ---- Linhagem ----
    _extraction_date                DATE        NOT NULL,
    _loaded_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (co_cnes, ano_mes)
);

CREATE INDEX IF NOT EXISTS ix_fato_covid_ano
    ON gold.fato_ocupacao_covid (ano_referencia);
CREATE INDEX IF NOT EXISTS ix_fato_covid_cnes
    ON gold.fato_ocupacao_covid (co_cnes);
CREATE INDEX IF NOT EXISTS ix_fato_covid_ano_mes
    ON gold.fato_ocupacao_covid (ano_mes);

COMMENT ON TABLE  gold.fato_ocupacao_covid IS
    'Ocupação hospitalar COVID-19 por estabelecimento × mês. JOIN co_cnes ⇄ gold.dim_estabelecimento; comparável com gold.fato_leitos_anual via co_cnes e ano_mes.';
COMMENT ON COLUMN gold.fato_ocupacao_covid.ano_mes IS
    'Competência mensal "YYYYMM" — formato idêntico ao comp de gold.fato_leitos_anual.';
COMMENT ON COLUMN gold.fato_ocupacao_covid.qtd_notificacoes IS
    'Nº de notificações no mês — proxy de "dias com declaração".';
COMMENT ON COLUMN gold.fato_ocupacao_covid.ocupacao_uti_covid_max IS
    'Pico mensal de UTI COVID ocupada — métrica chave para alertas epidemiológicos.';
COMMENT ON COLUMN gold.fato_ocupacao_covid.ocupacao_uti_hosp_max IS
    'Pico mensal de UTI ocupada total (não-COVID inclusive). Compara com leitos_existentes_max em fato_leitos_anual.';

-- ---------------------------------------------------------------------
-- ELT — recarga idempotente
-- ---------------------------------------------------------------------
TRUNCATE TABLE gold.fato_ocupacao_covid;

WITH latest AS (
    SELECT MAX(_extraction_date) AS d FROM silver.ocupacao_covid
)
INSERT INTO gold.fato_ocupacao_covid (
    co_cnes, ano_mes, ano_referencia, qtd_notificacoes,
    ocupacao_uti_covid_sum, ocupacao_uti_covid_avg, ocupacao_uti_covid_max,
    ocupacao_cli_covid_sum, ocupacao_cli_covid_avg, ocupacao_cli_covid_max,
    ocupacao_uti_hosp_max, ocupacao_cli_hosp_max,
    obitos_confirmados_sum, altas_confirmadas_sum,
    obitos_suspeitos_sum, altas_suspeitas_sum,
    _extraction_date
)
SELECT
    o.cnes                                              AS co_cnes,
    to_char(o.data_notificacao, 'YYYYMM')               AS ano_mes,
    o.ano_referencia,
    count(*)                                            AS qtd_notificacoes,
    sum(o.ocupacao_covid_uti)::bigint                   AS ocupacao_uti_covid_sum,
    round(avg(o.ocupacao_covid_uti)::numeric, 2)        AS ocupacao_uti_covid_avg,
    max(o.ocupacao_covid_uti)                           AS ocupacao_uti_covid_max,
    sum(o.ocupacao_covid_cli)::bigint                   AS ocupacao_cli_covid_sum,
    round(avg(o.ocupacao_covid_cli)::numeric, 2)        AS ocupacao_cli_covid_avg,
    max(o.ocupacao_covid_cli)                           AS ocupacao_cli_covid_max,
    max(o.ocupacao_hospitalar_uti)                      AS ocupacao_uti_hosp_max,
    max(o.ocupacao_hospitalar_cli)                      AS ocupacao_cli_hosp_max,
    sum(o.saida_confirmada_obitos)::bigint              AS obitos_confirmados_sum,
    sum(o.saida_confirmada_altas)::bigint               AS altas_confirmadas_sum,
    sum(o.saida_suspeita_obitos)::bigint                AS obitos_suspeitos_sum,
    sum(o.saida_suspeita_altas)::bigint                 AS altas_suspeitas_sum,
    latest.d                                            AS _extraction_date
FROM silver.ocupacao_covid o
JOIN latest ON o._extraction_date = latest.d
WHERE COALESCE(o.excluido, FALSE) = FALSE
GROUP BY o.cnes, to_char(o.data_notificacao, 'YYYYMM'), o.ano_referencia, latest.d;

ANALYZE gold.fato_ocupacao_covid;
