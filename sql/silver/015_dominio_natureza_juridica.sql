-- =====================================================================
-- silver.dominio_natureza_juridica — Decodificação de NATUREZA_JURIDICA
-- Fonte: Tabela CONCLA (Comissão Nacional de Classificação - Receita).
-- Curadoria manual cobrindo top 25 códigos (≈ 99% das linhas) + alguns
-- códigos comuns adicionais.
--
-- Códigos não cobertos retornam NULL no JOIN — incluir conforme aparecer.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.dominio_natureza_juridica (
    co_natureza_jur     TEXT        PRIMARY KEY,
    ds_natureza_jur     TEXT        NOT NULL,
    grupo               TEXT        NOT NULL,           -- PUBLICA, PRIVADA, SEM_FINS_LUCRATIVOS, PESSOA_FISICA
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE silver.dominio_natureza_juridica IS
    'Decodificação curada dos códigos CONCLA usados pelo CNES.';
COMMENT ON COLUMN silver.dominio_natureza_juridica.grupo IS
    'Agrupamento analítico: PUBLICA (1xxx), PRIVADA (2xxx), SEM_FINS_LUCRATIVOS (3xxx), PESSOA_FISICA (4xxx).';

TRUNCATE TABLE silver.dominio_natureza_juridica;

INSERT INTO silver.dominio_natureza_juridica (co_natureza_jur, ds_natureza_jur, grupo) VALUES
    -- 1xxx — ADMINISTRAÇÃO PÚBLICA
    ('1000', 'Administração Pública',                                      'PUBLICA'),
    ('1015', 'Órgão Público do Poder Executivo Federal',                   'PUBLICA'),
    ('1023', 'Órgão Público do Poder Executivo Estadual',                  'PUBLICA'),
    ('1031', 'Órgão Público do Poder Executivo Municipal',                 'PUBLICA'),
    ('1147', 'Autarquia Federal',                                          'PUBLICA'),
    ('1155', 'Autarquia Estadual',                                         'PUBLICA'),
    ('1163', 'Autarquia Municipal',                                        'PUBLICA'),
    ('1201', 'Fundação Pública de Direito Público Federal',                'PUBLICA'),
    ('1210', 'Fundação Pública de Direito Público Estadual',               'PUBLICA'),
    ('1228', 'Fundação Pública de Direito Público Municipal',              'PUBLICA'),
    ('1244', 'Município',                                                  'PUBLICA'),
    -- 2xxx — ENTIDADES EMPRESARIAIS (PRIVADAS)
    ('2000', 'Entidade Empresarial',                                       'PRIVADA'),
    ('2046', 'Sociedade Anônima Aberta',                                   'PRIVADA'),
    ('2054', 'Sociedade Anônima Fechada',                                  'PRIVADA'),
    ('2062', 'Sociedade Empresária Limitada (LTDA)',                       'PRIVADA'),
    ('2135', 'Empresa Individual de Responsabilidade Limitada (EIRELI)',   'PRIVADA'),
    ('2143', 'Empresa Individual Imobiliária',                             'PRIVADA'),
    ('2232', 'Cooperativa',                                                'PRIVADA'),
    ('2240', 'Sociedade Simples Pura',                                     'PRIVADA'),
    ('2305', 'Sociedade Simples Limitada',                                 'PRIVADA'),
    ('2313', 'Sociedade Simples em Nome Coletivo',                         'PRIVADA'),
    -- 3xxx — ENTIDADES SEM FINS LUCRATIVOS
    ('3000', 'Entidade Sem Fins Lucrativos',                               'SEM_FINS_LUCRATIVOS'),
    ('3069', 'Associação Privada',                                         'SEM_FINS_LUCRATIVOS'),
    ('3077', 'Serviço Social Autônomo',                                    'SEM_FINS_LUCRATIVOS'),
    ('3131', 'Organização Religiosa',                                      'SEM_FINS_LUCRATIVOS'),
    ('3204', 'Organização Social (OS)',                                    'SEM_FINS_LUCRATIVOS'),
    ('3999', 'Outras Entidades Sem Fins Lucrativos',                       'SEM_FINS_LUCRATIVOS'),
    -- 4xxx — PESSOAS FÍSICAS / NATURAIS
    ('4000', 'Pessoa Física (Consultório Isolado)',                        'PESSOA_FISICA'),
    ('4014', 'Microempreendedor Individual (MEI)',                         'PESSOA_FISICA'),
    ('4090', 'Contribuinte Individual',                                    'PESSOA_FISICA'),
    ('4146', 'Médico / Profissional Liberal',                              'PESSOA_FISICA');

ANALYZE silver.dominio_natureza_juridica;
