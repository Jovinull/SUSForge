"""Camada Bronze — ingestão crua e imutável de dados do OpenDATASUS.

Responsabilidades:
    - Download bruto dos arquivos do DATASUS (.dbc, .csv, .parquet).
    - Conversão `.dbc → .parquet` sem alteração semântica.
    - Carga em `schema bronze` com colunas de metadados
      (`_ingested_at`, `_source_file`, `_source_hash`).

Regra de ouro: NUNCA sobrescrever dado bronze. Particionar por
`dataset/uf/competencia/`.
"""
