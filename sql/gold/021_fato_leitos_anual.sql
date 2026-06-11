-- =====================================================================
-- gold.fato_leitos_anual — Fato de capacidade hospitalar por ano
-- Granularidade: (co_cnes, ano_referencia)
-- Origem: silver.leitos_anual (snapshot mais recente por _extraction_date)
-- Estratégia: TRUNCATE + INSERT — recriação total a cada execução
--
-- Métricas:
--   *_sum : leitos × meses-com-declaração (soma agregada)
--   *_avg : média mensal — métrica "leitos típicos do ano"
--   qtd_competencias : nº de meses com declaração (1..12)
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.fato_leitos_anual (
    co_cnes                     TEXT        NOT NULL,
    ano_referencia              INTEGER     NOT NULL,
    qtd_competencias            INTEGER     NOT NULL,
    -- SUMs ("leitos-meses-acumulados" — útil para indicadores agregados anuais)
    leitos_existentes_sum       BIGINT      NOT NULL,
    leitos_sus_sum              BIGINT      NOT NULL,
    uti_total_sum               BIGINT      NOT NULL,
    uti_sus_sum                 BIGINT      NOT NULL,
    -- AVGs (média mensal — "leitos típicos disponíveis no ano")
    leitos_existentes_avg       NUMERIC(10,2) NOT NULL,
    leitos_sus_avg              NUMERIC(10,2) NOT NULL,
    uti_total_avg               NUMERIC(10,2) NOT NULL,
    uti_sus_avg                 NUMERIC(10,2) NOT NULL,
    -- MAX (pico anual)
    leitos_existentes_max       INTEGER     NOT NULL,
    uti_total_max               INTEGER     NOT NULL,
    -- Linhagem
    _extraction_date            DATE        NOT NULL,
    _loaded_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (co_cnes, ano_referencia)
);

CREATE INDEX IF NOT EXISTS ix_fato_leitos_ano
    ON gold.fato_leitos_anual (ano_referencia);
CREATE INDEX IF NOT EXISTS ix_fato_leitos_cnes
    ON gold.fato_leitos_anual (co_cnes);

COMMENT ON TABLE  gold.fato_leitos_anual IS
    'Capacidade hospitalar por estabelecimento × ano. JOIN co_cnes ⇄ gold.dim_estabelecimento. Snapshot da Silver mais recente; recriado a cada ELT.';
COMMENT ON COLUMN gold.fato_leitos_anual.qtd_competencias IS
    'Nº de meses (1..12) em que o estabelecimento foi declarado naquele ano. Util para detectar fechamento/abertura/parcialidade.';
COMMENT ON COLUMN gold.fato_leitos_anual.leitos_existentes_sum IS
    'Soma das declarações mensais — "leitos × meses". NÃO confundir com "leitos totais": é métrica agregada para indicadores compostos.';
COMMENT ON COLUMN gold.fato_leitos_anual.leitos_existentes_avg IS
    'Média mensal dos leitos existentes no ano — leitura "leitos típicos disponíveis".';

-- ---------------------------------------------------------------------
-- ELT — recarga idempotente
-- ---------------------------------------------------------------------
TRUNCATE TABLE gold.fato_leitos_anual;

WITH latest AS (
    SELECT MAX(_extraction_date) AS d FROM silver.leitos_anual
)
INSERT INTO gold.fato_leitos_anual (
    co_cnes, ano_referencia, qtd_competencias,
    leitos_existentes_sum, leitos_sus_sum, uti_total_sum, uti_sus_sum,
    leitos_existentes_avg, leitos_sus_avg, uti_total_avg, uti_sus_avg,
    leitos_existentes_max, uti_total_max,
    _extraction_date
)
SELECT
    l.cnes                                      AS co_cnes,
    l.ano_referencia,
    count(*)                                    AS qtd_competencias,
    sum(l.leitos_existentes)::bigint            AS leitos_existentes_sum,
    sum(l.leitos_sus)::bigint                   AS leitos_sus_sum,
    sum(l.uti_total_exist)::bigint              AS uti_total_sum,
    sum(l.uti_total_sus)::bigint                AS uti_sus_sum,
    round(avg(l.leitos_existentes)::numeric, 2) AS leitos_existentes_avg,
    round(avg(l.leitos_sus)::numeric, 2)        AS leitos_sus_avg,
    round(avg(l.uti_total_exist)::numeric, 2)   AS uti_total_avg,
    round(avg(l.uti_total_sus)::numeric, 2)     AS uti_sus_avg,
    max(l.leitos_existentes)                    AS leitos_existentes_max,
    max(l.uti_total_exist)                      AS uti_total_max,
    latest.d                                    AS _extraction_date
FROM silver.leitos_anual l
JOIN latest ON l._extraction_date = latest.d
GROUP BY l.cnes, l.ano_referencia, latest.d;

ANALYZE gold.fato_leitos_anual;
