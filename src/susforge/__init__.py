"""SUSForge — Data Warehouse Medalhão para OpenDATASUS.

Pacote raiz. Subpacotes:
    - config  : configuração tipada via pydantic-settings.
    - io      : extração e carga (download, conversão DBC, load no Postgres).
    - bronze  : ingestão crua + metadados.
    - silver  : limpeza, tipagem e decodificação de domínios.
    - gold    : modelagem dimensional Kimball.
    - domain  : entidades e tabelas de domínio reutilizáveis.
"""

__version__ = "0.1.0"
