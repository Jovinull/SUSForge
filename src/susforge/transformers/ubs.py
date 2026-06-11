"""Transformer Silver para ``silver.ubs``.

Decisões específicas:
    * Coordenadas vêm com **vírgula decimal** brasileira
      (``"-43,9914089036"``); convertemos para ponto antes do cast.
    * Coordenadas fora do envelope Brasil ou ≈0 viram ``None``.
    * ``cnes`` já vem com zero-padding 7 dígitos da fonte — apenas trim.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import polars as pl

from susforge.schemas.ubs import (
    LAT_MAX,
    LAT_MIN,
    LON_MAX,
    LON_MIN,
    SILVER_COLUMN_ORDER,
    UbsSchema,
)

logger = logging.getLogger(__name__)


COLUMN_RENAMES: Final[dict[str, str]] = {
    "CNES": "cnes",
    "UF": "co_uf",
    "IBGE": "co_ibge",
    "NOME": "no_unidade",
    "LOGRADOURO": "no_logradouro",
    "BAIRRO": "no_bairro",
    "LATITUDE": "nu_latitude",
    "LONGITUDE": "nu_longitude",
}

NULL_STRINGS: Final[tuple[str, ...]] = (
    "",
    "NULL",
    "NULO",
    "NA",
    "N/A",
    "NONE",
    "NAN",
)


def _clean_string(col: str) -> pl.Expr:
    trimmed = pl.col(col).str.strip_chars()
    return (
        pl.when(trimmed.str.to_uppercase().is_in(NULL_STRINGS))
        .then(None)
        .otherwise(trimmed)
        .alias(col)
    )


def _to_geo(col: str, *, lo: float, hi: float) -> pl.Expr:
    """Vírgula decimal BR → ponto → Float64; zero/fora-envelope → None."""
    casted = (
        pl.col(col)
        .str.strip_chars()
        .str.replace_all(",", ".")
        .cast(pl.Float64, strict=False)
    )
    return (
        pl.when(
            casted.is_null()
            | (casted.abs() < 1e-6)
            | (casted < lo)
            | (casted > hi)
        )
        .then(None)
        .otherwise(casted)
        .alias(col)
    )


def _staging_partition(extraction_date: date) -> Path:
    from susforge.config import get_settings

    return (
        get_settings().data_dir
        / "staging"
        / "ubs"
        / "unidades"
        / extraction_date.isoformat()
    )


def transform(extraction_date: date) -> pl.DataFrame:
    """Lê o parquet UBS, normaliza, limpa, valida."""
    partition = _staging_partition(extraction_date)
    parquet = partition / "ubs.parquet"
    if not parquet.exists():
        raise FileNotFoundError(f"Parquet UBS ausente: {parquet}")

    logger.info("Lendo %s", parquet)
    df = pl.read_parquet(parquet)

    df = df.rename(COLUMN_RENAMES)

    # Limpeza de strings (todas exceto coordenadas e linhagem)
    string_cols = ("co_uf", "co_ibge", "no_unidade", "no_logradouro", "no_bairro")
    df = df.with_columns([_clean_string(c) for c in string_cols])

    # cnes: trim + zero-pad defensivo
    df = df.with_columns(
        pl.col("cnes").str.strip_chars().str.zfill(7).alias("cnes")
    )

    # Coordenadas com vírgula decimal BR → float com envelope Brasil
    df = df.with_columns(
        _to_geo("nu_latitude", lo=LAT_MIN, hi=LAT_MAX),
        _to_geo("nu_longitude", lo=LON_MIN, hi=LON_MAX),
    )

    # Filtra linhas sem cnes (defensivo)
    before = df.height
    df = df.filter(pl.col("cnes").is_not_null() & (pl.col("cnes") != "0000000"))
    if df.height < before:
        logger.warning("Removidas %d linhas sem cnes válido", before - df.height)

    # Dedup por cnes — fonte tem 1 linha por unidade, mas defensivo
    before = df.height
    df = df.unique(subset=["cnes"], keep="last", maintain_order=True)
    if df.height < before:
        logger.info("Dedup: removidas %d duplicatas (cnes)", before - df.height)

    # Linhagem Silver
    df = df.with_columns(
        pl.lit(datetime.now(tz=timezone.utc)).alias("_cleansed_at"),
        pl.lit(extraction_date).alias("_extraction_date"),
    )

    df = df.select(list(SILVER_COLUMN_ORDER))

    logger.info("Validando UbsSchema (%d × %d)…", df.height, df.width)
    UbsSchema.validate(df, lazy=False)
    logger.info("✓ schema validado")

    return df


__all__ = ["transform"]
