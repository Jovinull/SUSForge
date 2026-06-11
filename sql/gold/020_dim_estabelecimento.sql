-- =====================================================================
-- gold.dim_estabelecimento — Dimensão Master Data de estabelecimentos
-- Origem: silver.estabelecimentos (snapshot mais recente por _extraction_date)
-- PK: co_cnes (chave natural, 7 dígitos zero-padded para casar com a fato)
-- Estratégia: TRUNCATE + INSERT — recriação total a cada execução
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_estabelecimento (
    co_cnes                 TEXT        PRIMARY KEY,
    no_fantasia             TEXT,
    no_razao_social         TEXT,
    co_uf                   TEXT,
    sg_uf                   TEXT,           -- decodificado: SP, RJ, MG…
    ds_uf                   TEXT,           -- decodificado: São Paulo…
    co_ibge                 TEXT,
    tp_gestao               TEXT,           -- M (Municipal), E (Estadual), D (Dupla)
    tp_unidade              TEXT,           -- código bruto (2 dígitos zero-padded)
    ds_tipo_unidade         TEXT,           -- decodificado: "Hospital Geral", "UBS"…
    categoria_unidade       TEXT,           -- agrupamento: HOSPITALAR, ATENCAO_BASICA…
    co_natureza_jur         TEXT,           -- código CONCLA (4 dígitos)
    ds_natureza_juridica    TEXT,           -- decodificado: "Município", "LTDA"…
    grupo_natureza_jur      TEXT,           -- agrupamento: PUBLICA, PRIVADA…
    co_motivo_desab         TEXT,           -- NULL se ativo
    is_ativo                BOOLEAN GENERATED ALWAYS AS (co_motivo_desab IS NULL) STORED,
    is_ubs                  BOOLEAN     NOT NULL DEFAULT FALSE,
    nu_latitude             DOUBLE PRECISION,
    nu_longitude            DOUBLE PRECISION,
    geom                    GEOGRAPHY(Point, 4326),         -- PostGIS — WGS84
    co_cep                  TEXT,
    st_atend_hospitalar     BOOLEAN,
    st_centro_cirurgico     BOOLEAN,
    st_atend_ambulatorial   BOOLEAN,
    _extraction_date        DATE        NOT NULL,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Migrações idempotentes (caso a tabela exista de versões anteriores)
ALTER TABLE gold.dim_estabelecimento
    ADD COLUMN IF NOT EXISTS is_ubs BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE gold.dim_estabelecimento
    ADD COLUMN IF NOT EXISTS geom GEOGRAPHY(Point, 4326);
ALTER TABLE gold.dim_estabelecimento
    ADD COLUMN IF NOT EXISTS ds_tipo_unidade TEXT;
ALTER TABLE gold.dim_estabelecimento
    ADD COLUMN IF NOT EXISTS categoria_unidade TEXT;
ALTER TABLE gold.dim_estabelecimento
    ADD COLUMN IF NOT EXISTS ds_natureza_juridica TEXT;
ALTER TABLE gold.dim_estabelecimento
    ADD COLUMN IF NOT EXISTS grupo_natureza_jur TEXT;

CREATE INDEX IF NOT EXISTS ix_dim_estab_sg_uf
    ON gold.dim_estabelecimento (sg_uf);
CREATE INDEX IF NOT EXISTS ix_dim_estab_co_ibge
    ON gold.dim_estabelecimento (co_ibge);
CREATE INDEX IF NOT EXISTS ix_dim_estab_ativo
    ON gold.dim_estabelecimento (is_ativo)
    WHERE is_ativo;
CREATE INDEX IF NOT EXISTS ix_dim_estab_is_ubs
    ON gold.dim_estabelecimento (is_ubs)
    WHERE is_ubs;
CREATE INDEX IF NOT EXISTS ix_dim_estab_geom
    ON gold.dim_estabelecimento USING GIST (geom);
CREATE INDEX IF NOT EXISTS ix_dim_estab_categoria
    ON gold.dim_estabelecimento (categoria_unidade);
CREATE INDEX IF NOT EXISTS ix_dim_estab_grupo_natj
    ON gold.dim_estabelecimento (grupo_natureza_jur);

COMMENT ON TABLE  gold.dim_estabelecimento IS
    'Dimensão Master Data dos estabelecimentos de saúde — snapshot da Silver mais recente; recriada a cada ELT.';
COMMENT ON COLUMN gold.dim_estabelecimento.co_cnes IS
    'CNES com 7 dígitos zero-padded (lpad). Casamento direto com gold.fato_leitos_anual.';
COMMENT ON COLUMN gold.dim_estabelecimento.sg_uf IS
    'Sigla da UF decodificada do código IBGE (11=RO, 35=SP, …).';
COMMENT ON COLUMN gold.dim_estabelecimento.is_ativo IS
    'Flag derivada: TRUE se co_motivo_desab IS NULL (estabelecimento ativo).';
COMMENT ON COLUMN gold.dim_estabelecimento.is_ubs IS
    'TRUE se o estabelecimento aparece em silver.ubs (Unidade Básica de Saúde / Atenção Primária).';
COMMENT ON COLUMN gold.dim_estabelecimento.geom IS
    'Geografia WGS84 (SRID 4326). NULL quando coordenadas ausentes ou fora do envelope Brasil. Use ST_DWithin para queries de raio.';
COMMENT ON COLUMN gold.dim_estabelecimento.ds_tipo_unidade IS
    'Decodificação de tp_unidade via silver.dominio_tp_unidade. NULL para códigos não curados.';
COMMENT ON COLUMN gold.dim_estabelecimento.categoria_unidade IS
    'Agrupamento analítico do tipo de unidade: ATENCAO_BASICA, HOSPITALAR, AMBULATORIAL, APOIO, GESTAO, OUTROS.';
COMMENT ON COLUMN gold.dim_estabelecimento.ds_natureza_juridica IS
    'Decodificação de co_natureza_jur via silver.dominio_natureza_juridica (CONCLA).';
COMMENT ON COLUMN gold.dim_estabelecimento.grupo_natureza_jur IS
    'Agrupamento analítico: PUBLICA (1xxx), PRIVADA (2xxx), SEM_FINS_LUCRATIVOS (3xxx), PESSOA_FISICA (4xxx).';

-- ---------------------------------------------------------------------
-- ELT — recarga idempotente
-- ---------------------------------------------------------------------
TRUNCATE TABLE gold.dim_estabelecimento;

WITH latest AS (
    SELECT MAX(_extraction_date) AS d FROM silver.estabelecimentos
),
uf_map(co_uf, sg_uf, ds_uf) AS (
    VALUES
        ('11','RO','Rondônia'),
        ('12','AC','Acre'),
        ('13','AM','Amazonas'),
        ('14','RR','Roraima'),
        ('15','PA','Pará'),
        ('16','AP','Amapá'),
        ('17','TO','Tocantins'),
        ('21','MA','Maranhão'),
        ('22','PI','Piauí'),
        ('23','CE','Ceará'),
        ('24','RN','Rio Grande do Norte'),
        ('25','PB','Paraíba'),
        ('26','PE','Pernambuco'),
        ('27','AL','Alagoas'),
        ('28','SE','Sergipe'),
        ('29','BA','Bahia'),
        ('31','MG','Minas Gerais'),
        ('32','ES','Espírito Santo'),
        ('33','RJ','Rio de Janeiro'),
        ('35','SP','São Paulo'),
        ('41','PR','Paraná'),
        ('42','SC','Santa Catarina'),
        ('43','RS','Rio Grande do Sul'),
        ('50','MS','Mato Grosso do Sul'),
        ('51','MT','Mato Grosso'),
        ('52','GO','Goiás'),
        ('53','DF','Distrito Federal')
)
INSERT INTO gold.dim_estabelecimento (
    co_cnes, no_fantasia, no_razao_social,
    co_uf, sg_uf, ds_uf, co_ibge,
    tp_gestao,
    tp_unidade, ds_tipo_unidade, categoria_unidade,
    co_natureza_jur, ds_natureza_juridica, grupo_natureza_jur,
    co_motivo_desab,
    is_ubs,
    nu_latitude, nu_longitude, geom,
    co_cep,
    st_atend_hospitalar, st_centro_cirurgico, st_atend_ambulatorial,
    _extraction_date
)
SELECT
    lpad(e.co_cnes, 7, '0')       AS co_cnes,
    e.no_fantasia,
    e.no_razao_social,
    e.co_uf,
    u.sg_uf,
    u.ds_uf,
    e.co_ibge,
    e.tp_gestao,
    lpad(e.tp_unidade, 2, '0')    AS tp_unidade,
    dtp.ds_tp_unidade             AS ds_tipo_unidade,
    dtp.categoria                 AS categoria_unidade,
    e.co_natureza_jur,
    dnj.ds_natureza_jur           AS ds_natureza_juridica,
    dnj.grupo                     AS grupo_natureza_jur,
    e.co_motivo_desab,
    (ubs.cnes IS NOT NULL)        AS is_ubs,
    e.nu_latitude,
    e.nu_longitude,
    CASE
        WHEN e.nu_latitude IS NOT NULL AND e.nu_longitude IS NOT NULL
        THEN ST_SetSRID(ST_MakePoint(e.nu_longitude, e.nu_latitude), 4326)::geography
        ELSE NULL
    END                           AS geom,
    e.co_cep,
    e.st_atend_hospitalar,
    e.st_centro_cirurgico,
    e.st_atend_ambulatorial,
    e._extraction_date
FROM silver.estabelecimentos e
LEFT JOIN uf_map u ON u.co_uf = e.co_uf
LEFT JOIN (
    -- Snapshot mais recente de UBS (1 linha por cnes)
    SELECT DISTINCT cnes
    FROM silver.ubs
    WHERE _extraction_date = (SELECT MAX(_extraction_date) FROM silver.ubs)
) ubs ON ubs.cnes = lpad(e.co_cnes, 7, '0')
LEFT JOIN silver.dominio_tp_unidade        dtp ON dtp.co_tp_unidade   = lpad(e.tp_unidade, 2, '0')
LEFT JOIN silver.dominio_natureza_juridica dnj ON dnj.co_natureza_jur = e.co_natureza_jur
JOIN latest ON e._extraction_date = latest.d;

ANALYZE gold.dim_estabelecimento;
