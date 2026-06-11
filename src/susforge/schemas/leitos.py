"""Contrato Pandera para ``silver.leitos_anual``.

Granularidade: 1 linha por ``(_extraction_date, comp, cnes)``.
``comp`` é a competência mensal (``"YYYYMM"``); ``ano_referencia`` é o
inteiro derivado dela. Quantitativos são ``int`` não-nulos (0 quando o
estabelecimento não declarou) — viabilizando ``SUM`` direto na Gold.
"""

from __future__ import annotations

from typing import Annotated

import polars as pl
from pandera.polars import DataFrameModel, Field
from pandera.typing.polars import Series

DatetimeUTC = Annotated[pl.Datetime, "us", "UTC"]


class LeitosAnualSchema(DataFrameModel):
    """Schema da tabela ``silver.leitos_anual``."""

    # ---- Tempo ----
    ano_referencia: Series[int] = Field(nullable=False, ge=2000, le=2100)
    comp: Series[str] = Field(nullable=False, str_matches=r"^\d{6}$")

    # ---- Estabelecimento ----
    cnes: Series[str] = Field(nullable=False)
    nome_estabelecimento: Series[str] = Field(nullable=True)
    razao_social: Series[str] = Field(nullable=True)
    motivo_desabilitacao: Series[str] = Field(nullable=True)
    tp_gestao: Series[str] = Field(nullable=True)
    co_tipo_unidade: Series[str] = Field(nullable=True)
    ds_tipo_unidade: Series[str] = Field(nullable=True)
    natureza_juridica: Series[str] = Field(nullable=True)
    desc_natureza_juridica: Series[str] = Field(nullable=True)

    # ---- Localização ----
    regiao: Series[str] = Field(nullable=True)
    uf: Series[str] = Field(nullable=True)
    municipio: Series[str] = Field(nullable=True)

    # ---- Endereço ----
    no_logradouro: Series[str] = Field(nullable=True)
    nu_endereco: Series[str] = Field(nullable=True)
    no_complemento: Series[str] = Field(nullable=True)
    no_bairro: Series[str] = Field(nullable=True)
    co_cep: Series[str] = Field(nullable=True)
    nu_telefone: Series[str] = Field(nullable=True)
    no_email: Series[str] = Field(nullable=True)

    # ---- Quantitativos (FATO — int não-nulo) ----
    leitos_existentes: Series[int] = Field(nullable=False, ge=0)
    leitos_sus: Series[int] = Field(nullable=False, ge=0)
    uti_total_exist: Series[int] = Field(nullable=False, ge=0)
    uti_total_sus: Series[int] = Field(nullable=False, ge=0)
    uti_adulto_exist: Series[int] = Field(nullable=False, ge=0)
    uti_adulto_sus: Series[int] = Field(nullable=False, ge=0)
    uti_pediatrico_exist: Series[int] = Field(nullable=False, ge=0)
    uti_pediatrico_sus: Series[int] = Field(nullable=False, ge=0)
    uti_neonatal_exist: Series[int] = Field(nullable=False, ge=0)
    uti_neonatal_sus: Series[int] = Field(nullable=False, ge=0)
    uti_queimado_exist: Series[int] = Field(nullable=False, ge=0)
    uti_queimado_sus: Series[int] = Field(nullable=False, ge=0)
    uti_coronariana_exist: Series[int] = Field(nullable=False, ge=0)
    uti_coronariana_sus: Series[int] = Field(nullable=False, ge=0)

    # ---- Linhagem ----
    ingested_at: Series[DatetimeUTC] = Field(
        nullable=False, alias="_ingested_at"
    )
    source_file: Series[str] = Field(nullable=False, alias="_source_file")
    source_hash: Series[str] = Field(nullable=False, alias="_source_hash")
    extraction_date: Series[pl.Date] = Field(
        nullable=False, alias="_extraction_date"
    )
    cleansed_at: Series[DatetimeUTC] = Field(
        nullable=False, alias="_cleansed_at"
    )

    class Config:
        strict = True


SILVER_COLUMN_ORDER: tuple[str, ...] = (
    "ano_referencia",
    "comp",
    "cnes",
    "nome_estabelecimento",
    "razao_social",
    "motivo_desabilitacao",
    "tp_gestao",
    "co_tipo_unidade",
    "ds_tipo_unidade",
    "natureza_juridica",
    "desc_natureza_juridica",
    "regiao",
    "uf",
    "municipio",
    "no_logradouro",
    "nu_endereco",
    "no_complemento",
    "no_bairro",
    "co_cep",
    "nu_telefone",
    "no_email",
    "leitos_existentes",
    "leitos_sus",
    "uti_total_exist",
    "uti_total_sus",
    "uti_adulto_exist",
    "uti_adulto_sus",
    "uti_pediatrico_exist",
    "uti_pediatrico_sus",
    "uti_neonatal_exist",
    "uti_neonatal_sus",
    "uti_queimado_exist",
    "uti_queimado_sus",
    "uti_coronariana_exist",
    "uti_coronariana_sus",
    "_ingested_at",
    "_source_file",
    "_source_hash",
    "_extraction_date",
    "_cleansed_at",
)

# Colunas FATO (quantitativos) — usadas para fill_null(0) + cast int.
QUANTITATIVE_COLUMNS: tuple[str, ...] = (
    "leitos_existentes",
    "leitos_sus",
    "uti_total_exist",
    "uti_total_sus",
    "uti_adulto_exist",
    "uti_adulto_sus",
    "uti_pediatrico_exist",
    "uti_pediatrico_sus",
    "uti_neonatal_exist",
    "uti_neonatal_sus",
    "uti_queimado_exist",
    "uti_queimado_sus",
    "uti_coronariana_exist",
    "uti_coronariana_sus",
)


__all__ = [
    "QUANTITATIVE_COLUMNS",
    "SILVER_COLUMN_ORDER",
    "LeitosAnualSchema",
]
