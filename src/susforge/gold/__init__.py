"""Camada Gold — modelagem dimensional Kimball para BI.

Responsabilidades:
    - Construção de star schemas (`fato_*`, `dim_*`, `agg_*`).
    - Geração de chaves substitutas (`sk_*`).
    - Dimensões conformes compartilhadas entre fatos.
    - Materializações otimizadas para queries do Metabase.
"""
