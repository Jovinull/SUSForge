"""Camada Silver — dados limpos, tipados e com domínios decodificados.

Responsabilidades:
    - Tipagem estrita (datas, decimais, booleanos, categóricos).
    - Decodificação de tabelas de domínio (CID, CBO, CNES, IBGE).
    - Normalização de chaves (`cod_municipio_ibge` 7 dígitos, `co_cnes`).
    - Deduplicação e validações declarativas via `pandera`.
"""
