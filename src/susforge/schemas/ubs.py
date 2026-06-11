"""Contrato Pandera para ``silver.ubs``.

Unidades Básicas de Saúde — subset do CNES focado em Atenção Primária.
8 colunas de dado + linhagem.

Granularidade: 1 linha por ``(_extraction_date, cnes)``.
"""

from __future__ import annotations

from typing import Annotated

import polars as pl
from pandera.polars import DataFrameModel, Field
from pandera.typing.polars import Series

DatetimeUTC = Annotated[pl.Datetime, "us", "UTC"]

# Envelope geográfico do Brasil (mesmas constantes que CNES).
LAT_MIN: float = -35.0
LAT_MAX: float = 6.0
LON_MIN: float = -75.0
LON_MAX: float = -33.0


class UbsSchema(DataFrameModel):
    """Schema da tabela ``silver.ubs``."""

    cnes: Series[str] = Field(nullable=False)
    co_uf: Series[str] = Field(nullable=True)
    co_ibge: Series[str] = Field(nullable=True)
    no_unidade: Series[str] = Field(nullable=True)
    no_logradouro: Series[str] = Field(nullable=True)
    no_bairro: Series[str] = Field(nullable=True)
    nu_latitude: Series[float] = Field(nullable=True, ge=LAT_MIN, le=LAT_MAX)
    nu_longitude: Series[float] = Field(nullable=True, ge=LON_MIN, le=LON_MAX)
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
    "cnes",
    "co_uf",
    "co_ibge",
    "no_unidade",
    "no_logradouro",
    "no_bairro",
    "nu_latitude",
    "nu_longitude",
    "_ingested_at",
    "_source_file",
    "_source_hash",
    "_extraction_date",
    "_cleansed_at",
)


__all__ = [
    "LAT_MAX",
    "LAT_MIN",
    "LON_MAX",
    "LON_MIN",
    "SILVER_COLUMN_ORDER",
    "UbsSchema",
]
