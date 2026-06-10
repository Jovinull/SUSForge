"""Entrada e saída — download, conversão e carga.

Responsabilidades:
    - Cliente HTTP para o OpenDATASUS (FTP/HTTPS).
    - Conversores de formato (`.dbc → .parquet`).
    - Adaptadores de carga para Postgres (COPY, INSERT em lote via Polars).

Observação: este subpacote (`susforge.io`) NÃO colide com o módulo
`io` da stdlib — sempre importe explicitamente como `susforge.io`.
"""
