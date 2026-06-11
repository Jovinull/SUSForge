"""Transformer Silver para ``silver.estabelecimentos``.

Lê o Parquet aterrissado pela Bronze, aplica limpeza determinística,
adiciona linhagem temporal (``_cleansed_at``, ``_extraction_date``) e
valida o resultado com Pandera **antes** de devolver ao loader.

Decisões:
    * Nomes de coluna → ``snake_case`` lowercase (contrato com DDL).
    * Strings: ``trim`` + sentinelas ``"NULL"``/``"NULO"``/vazias → ``None``.
    * ``st_*`` (flags ``"1.0"``/``"0.0"``): cast Float → Bool.
    * ``nu_latitude``/``nu_longitude``: cast Float; zera (= None) quando
      ausentes, no Atlântico (``0.0``) ou fora do envelope Brasil.
    * ``co_ibge``: trim apenas — preserva 6 ou 7 dígitos conforme vier.

Não decodificamos domínios (TP_GESTAO, NATUREZA_*) nesta camada — isso
fica para Gold ou para tabelas auxiliares de domínio.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import polars as pl

from susforge.schemas.cnes import (
    LAT_MAX,
    LAT_MIN,
    LON_MAX,
    LON_MIN,
    SILVER_COLUMN_ORDER,
    EstabelecimentosSchema,
)

logger = logging.getLogger(__name__)


# Bronze → Silver: nomes em snake_case lowercase.
COLUMN_RENAMES: Final[dict[str, str]] = {
    "CO_CNES": "co_cnes",
    "CO_UNIDADE": "co_unidade",
    "CO_UF": "co_uf",
    "CO_IBGE": "co_ibge",
    "NU_CNPJ_MANTENEDORA": "nu_cnpj_mantenedora",
    "NO_RAZAO_SOCIAL": "no_razao_social",
    "NO_FANTASIA": "no_fantasia",
    "CO_NATUREZA_ORGANIZACAO": "co_natureza_organizacao",
    "DS_NATUREZA_ORGANIZACAO": "ds_natureza_organizacao",
    "TP_GESTAO": "tp_gestao",
    "CO_NIVEL_HIERARQUIA": "co_nivel_hierarquia",
    "DS_NIVEL_HIERARQUIA": "ds_nivel_hierarquia",
    "CO_ESFERA_ADMINISTRATIVA": "co_esfera_administrativa",
    "DS_ESFERA_ADMINISTRATIVA": "ds_esfera_administrativa",
    "CO_ATIVIDADE": "co_atividade",
    "TP_UNIDADE": "tp_unidade",
    "CO_CEP": "co_cep",
    "NO_LOGRADOURO": "no_logradouro",
    "NU_ENDERECO": "nu_endereco",
    "NO_BAIRRO": "no_bairro",
    "NU_TELEFONE": "nu_telefone",
    "NU_LATITUDE": "nu_latitude",
    "NU_LONGITUDE": "nu_longitude",
    "CO_TURNO_ATENDIMENTO": "co_turno_atendimento",
    "DS_TURNO_ATENDIMENTO": "ds_turno_atendimento",
    "NU_CNPJ": "nu_cnpj",
    "NO_EMAIL": "no_email",
    "CO_NATUREZA_JUR": "co_natureza_jur",
    "ST_CENTRO_CIRURGICO": "st_centro_cirurgico",
    "ST_CENTRO_OBSTETRICO": "st_centro_obstetrico",
    "ST_CENTRO_NEONATAL": "st_centro_neonatal",
    "ST_ATEND_HOSPITALAR": "st_atend_hospitalar",
    "ST_SERVICO_APOIO": "st_servico_apoio",
    "ST_ATEND_AMBULATORIAL": "st_atend_ambulatorial",
    "CO_MOTIVO_DESAB": "co_motivo_desab",
    "CO_AMBULATORIAL_SUS": "co_ambulatorial_sus",
}

BOOL_COLUMNS: Final[tuple[str, ...]] = (
    "st_centro_cirurgico",
    "st_centro_obstetrico",
    "st_centro_neonatal",
    "st_atend_hospitalar",
    "st_servico_apoio",
    "st_atend_ambulatorial",
)
FLOAT_COLUMNS: Final[tuple[str, ...]] = ("nu_latitude", "nu_longitude")

# Sentinelas de "string vazia / nulo mal formatado" típicas do DATASUS.
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
    """Trim + uppercase compare → None nas sentinelas."""
    trimmed = pl.col(col).str.strip_chars()
    return pl.when(trimmed.str.to_uppercase().is_in(NULL_STRINGS)).then(
        None
    ).otherwise(trimmed).alias(col)


def _to_bool(col: str) -> pl.Expr:
    """``"1.0"`` / ``"0.0"`` (string) → Bool. None preservado."""
    return (
        pl.col(col)
        .cast(pl.Float64, strict=False)
        .pipe(
            lambda c: pl.when(c.is_null())
            .then(None)
            .when(c > 0.5)
            .then(True)
            .otherwise(False)
        )
        .alias(col)
    )


def _to_geo(col: str, *, lo: float, hi: float) -> pl.Expr:
    """String → Float; zera quando placeholder (≈0) ou fora do envelope."""
    casted = pl.col(col).cast(pl.Float64, strict=False)
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


def transform(parquet_path: Path, extraction_date: date) -> pl.DataFrame:
    """Lê parquet Bronze, limpa, tipa, valida (Pandera) e retorna.

    Args:
        parquet_path: Caminho do Parquet em ``data/staging/cnes/...``.
        extraction_date: Data da partição Bronze (vira ``_extraction_date``).

    Returns:
        DataFrame Polars validado contra ``EstabelecimentosSchema``,
        com colunas na ordem exata de ``SILVER_COLUMN_ORDER``.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet bronze ausente: {parquet_path}")

    logger.info("Lendo parquet %s", parquet_path)
    df = pl.read_parquet(parquet_path)

    # 1) Renomear → snake_case lowercase
    df = df.rename(COLUMN_RENAMES)
    logger.info("Renomeadas %d colunas", len(COLUMN_RENAMES))

    # 2) Limpar strings — todas as colunas Utf8 originais (exceto linhagem)
    string_cols = [
        name
        for name, dtype in df.schema.items()
        if dtype == pl.String
        and not name.startswith("_")
        and name not in BOOL_COLUMNS
        and name not in FLOAT_COLUMNS
    ]
    df = df.with_columns([_clean_string(c) for c in string_cols])

    # 3) Cast booleans
    df = df.with_columns([_to_bool(c) for c in BOOL_COLUMNS])

    # 4) Cast coordenadas (com envelope Brasil)
    df = df.with_columns(
        _to_geo("nu_latitude", lo=LAT_MIN, hi=LAT_MAX),
        _to_geo("nu_longitude", lo=LON_MIN, hi=LON_MAX),
    )

    # 5) Linhagem Silver
    df = df.with_columns(
        pl.lit(datetime.now(tz=timezone.utc)).alias("_cleansed_at"),
        pl.lit(extraction_date).alias("_extraction_date"),
    )

    # 6) Ordem canônica das colunas (mesma do DDL e do COPY)
    df = df.select(list(SILVER_COLUMN_ORDER))

    # 7) Validação Pandera (lazy=False → falha no primeiro erro)
    logger.info(
        "Validando contra EstabelecimentosSchema (%d linhas, %d cols)…",
        df.height,
        df.width,
    )
    EstabelecimentosSchema.validate(df, lazy=False)
    logger.info("✓ schema validado")

    return df


__all__ = ["transform"]
