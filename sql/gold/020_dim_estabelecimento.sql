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
    tp_unidade              TEXT,
    co_natureza_jur         TEXT,
    co_motivo_desab         TEXT,           -- NULL se ativo
    is_ativo                BOOLEAN GENERATED ALWAYS AS (co_motivo_desab IS NULL) STORED,
    nu_latitude             DOUBLE PRECISION,
    nu_longitude            DOUBLE PRECISION,
    co_cep                  TEXT,
    st_atend_hospitalar     BOOLEAN,
    st_centro_cirurgico     BOOLEAN,
    st_atend_ambulatorial   BOOLEAN,
    _extraction_date        DATE        NOT NULL,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dim_estab_sg_uf
    ON gold.dim_estabelecimento (sg_uf);
CREATE INDEX IF NOT EXISTS ix_dim_estab_co_ibge
    ON gold.dim_estabelecimento (co_ibge);
CREATE INDEX IF NOT EXISTS ix_dim_estab_ativo
    ON gold.dim_estabelecimento (is_ativo)
    WHERE is_ativo;

COMMENT ON TABLE  gold.dim_estabelecimento IS
    'Dimensão Master Data dos estabelecimentos de saúde — snapshot da Silver mais recente; recriada a cada ELT.';
COMMENT ON COLUMN gold.dim_estabelecimento.co_cnes IS
    'CNES com 7 dígitos zero-padded (lpad). Casamento direto com gold.fato_leitos_anual.';
COMMENT ON COLUMN gold.dim_estabelecimento.sg_uf IS
    'Sigla da UF decodificada do código IBGE (11=RO, 35=SP, …).';
COMMENT ON COLUMN gold.dim_estabelecimento.is_ativo IS
    'Flag derivada: TRUE se co_motivo_desab IS NULL (estabelecimento ativo).';

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
    tp_gestao, tp_unidade, co_natureza_jur, co_motivo_desab,
    nu_latitude, nu_longitude, co_cep,
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
    e.tp_unidade,
    e.co_natureza_jur,
    e.co_motivo_desab,
    e.nu_latitude,
    e.nu_longitude,
    e.co_cep,
    e.st_atend_hospitalar,
    e.st_centro_cirurgico,
    e.st_atend_ambulatorial,
    e._extraction_date
FROM silver.estabelecimentos e
LEFT JOIN uf_map u ON u.co_uf = e.co_uf
JOIN latest ON e._extraction_date = latest.d;

ANALYZE gold.dim_estabelecimento;
