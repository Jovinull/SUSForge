-- =====================================================================
-- silver.dominio_tp_unidade — Decodificação de TP_UNIDADE (CNES)
-- Fonte: padrão DATASUS — Wiki CNES (https://wiki.saude.gov.br/cnes/)
-- Curadoria manual cobrindo top 30 códigos (≈ 99,9% das linhas).
-- Códigos não cobertos retornam NULL no JOIN — adicionar ao crescer.
--
-- Padronização: armazenamos com lpad(2, '0'). Códigos "2", "4" da
-- fonte viram "02", "04" no JOIN.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.dominio_tp_unidade (
    co_tp_unidade   TEXT        PRIMARY KEY,
    ds_tp_unidade   TEXT        NOT NULL,
    categoria       TEXT        NOT NULL,
    _loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE silver.dominio_tp_unidade IS
    'Decodificação curada dos códigos TP_UNIDADE do CNES. ~99,9% de cobertura sobre silver.estabelecimentos.';
COMMENT ON COLUMN silver.dominio_tp_unidade.categoria IS
    'Agrupamento analítico: ATENCAO_BASICA, HOSPITALAR, AMBULATORIAL, APOIO, GESTAO, OUTROS.';

TRUNCATE TABLE silver.dominio_tp_unidade;

INSERT INTO silver.dominio_tp_unidade (co_tp_unidade, ds_tp_unidade, categoria) VALUES
    ('01', 'Posto de Saúde',                                              'ATENCAO_BASICA'),
    ('02', 'Centro de Saúde / Unidade Básica',                            'ATENCAO_BASICA'),
    ('04', 'Policlínica',                                                 'AMBULATORIAL'),
    ('05', 'Hospital Geral',                                              'HOSPITALAR'),
    ('07', 'Hospital Especializado',                                      'HOSPITALAR'),
    ('09', 'Pronto Socorro Geral',                                        'HOSPITALAR'),
    ('12', 'Maternidade',                                                 'HOSPITALAR'),
    ('15', 'Unidade Mista',                                               'HOSPITALAR'),
    ('20', 'Pronto Socorro Especializado',                                'HOSPITALAR'),
    ('22', 'Consultório Isolado',                                         'AMBULATORIAL'),
    ('36', 'Clínica / Centro de Especialidade',                           'AMBULATORIAL'),
    ('39', 'Unidade de Apoio Diagnose e Terapia (SADT Isolado)',          'APOIO'),
    ('40', 'Unidade Móvel Terrestre',                                     'APOIO'),
    ('42', 'Unidade Móvel de Nível Pré-Hospitalar (Urgência)',            'APOIO'),
    ('43', 'Farmácia',                                                    'APOIO'),
    ('50', 'Unidade de Vigilância em Saúde',                              'APOIO'),
    ('60', 'Cooperativa ou Empresa de Cessão de Trabalhadores',           'OUTROS'),
    ('61', 'Centro de Parto Normal - Isolado',                            'HOSPITALAR'),
    ('62', 'Hospital/Dia - Isolado',                                      'HOSPITALAR'),
    ('67', 'Laboratório Central de Saúde Pública (LACEN)',                'APOIO'),
    ('68', 'Central de Gestão em Saúde',                                  'GESTAO'),
    ('69', 'Centro de Atenção Hemoterápica e/ou Hematológica',            'APOIO'),
    ('70', 'Centro de Atenção Psicossocial (CAPS)',                       'ATENCAO_BASICA'),
    ('71', 'Centro de Apoio à Saúde da Família',                          'ATENCAO_BASICA'),
    ('72', 'Unidade de Atenção à Saúde Indígena',                         'ATENCAO_BASICA'),
    ('73', 'Pronto Atendimento (UPA)',                                    'HOSPITALAR'),
    ('74', 'Polo Academia da Saúde',                                      'ATENCAO_BASICA'),
    ('75', 'Telessaúde',                                                  'APOIO'),
    ('76', 'Central de Regulação Médica das Urgências',                   'GESTAO'),
    ('77', 'Serviço de Atenção Domiciliar Isolado',                       'AMBULATORIAL'),
    ('78', 'Unidade de Atenção em Regime Residencial',                    'HOSPITALAR'),
    ('79', 'Oficina Ortopédica',                                          'APOIO'),
    ('80', 'Laboratório de Saúde Pública',                                'APOIO'),
    ('81', 'Central de Regulação do Acesso',                              'GESTAO'),
    ('82', 'Central de Notificação, Captação e Distribuição de Órgãos',   'GESTAO'),
    ('83', 'Polo de Prevenção de Doenças e Promoção da Saúde',            'ATENCAO_BASICA'),
    ('84', 'Central de Abastecimento',                                    'APOIO'),
    ('85', 'Centro de Imunização',                                        'ATENCAO_BASICA');

ANALYZE silver.dominio_tp_unidade;
