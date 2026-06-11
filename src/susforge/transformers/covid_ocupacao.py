"""Transformer Silver para ``silver.ocupacao_covid``.

Consolida os 3 parquets bronze (2020–2022) em um único DataFrame
tipado e validado. Ações principais:

    * Descarta colunas técnicas sem valor analítico (``""`` anônima e
      ``_p_usuario`` do Parse).
    * Renomeia ``camelCase`` → ``snake_case``.
    * **Datas**: parse ISO 8601 (``2022-01-17T03:00:00.000Z``) →
      ``Datetime(us, UTC)``.
    * **Quantitativos** (``"13.0"``/``"0.0"``): cast Float → Int64 com
      ``fill_null(0)``.
    * **Booleanos** (``"True"``/``"False"`` string): cast para Bool.
    * Deriva ``ano_referencia`` da ``data_notificacao``.
    * Padroniza ``cnes`` com ``lpad(7,'0')`` para casar com a Gold.
    * Dedup por ``id_registro`` (defensivo — Parse pode reapresentar
      o mesmo evento em arquivos consecutivos).
    * Filtra linhas sem ``id_registro``/``cnes``/``data_notificacao``
      válidos.
    * Valida o resultado contra ``OcupacaoCovidSchema``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import polars as pl

from susforge.schemas.covid_ocupacao import (
    QUANTITATIVE_COLUMNS,
    SILVER_COLUMN_ORDER,
    OcupacaoCovidSchema,
)

logger = logging.getLogger(__name__)


# camelCase do e-SUS Notifica → snake_case canônico
COLUMN_RENAMES: Final[dict[str, str]] = {
    "_id": "id_registro",
    "dataNotificacao": "data_notificacao",
    "cnes": "cnes",
    "ocupacaoSuspeitoCli": "ocupacao_suspeito_cli",
    "ocupacaoSuspeitoUti": "ocupacao_suspeito_uti",
    "ocupacaoConfirmadoCli": "ocupacao_confirmado_cli",
    "ocupacaoConfirmadoUti": "ocupacao_confirmado_uti",
    "ocupacaoCovidUti": "ocupacao_covid_uti",
    "ocupacaoCovidCli": "ocupacao_covid_cli",
    "ocupacaoHospitalarUti": "ocupacao_hospitalar_uti",
    "ocupacaoHospitalarCli": "ocupacao_hospitalar_cli",
    "saidaSuspeitaObitos": "saida_suspeita_obitos",
    "saidaSuspeitaAltas": "saida_suspeita_altas",
    "saidaConfirmadaObitos": "saida_confirmada_obitos",
    "saidaConfirmadaAltas": "saida_confirmada_altas",
    "origem": "origem",
    "estadoNotificacao": "estado_notificacao",
    "municipioNotificacao": "municipio_notificacao",
    "estado": "estado",
    "municipio": "municipio",
    "excluido": "excluido",
    "validado": "validado",
    "_created_at": "registro_created_at",
    "_updated_at": "registro_updated_at",
}

# Colunas a descartar (lixo do CSV/Parse).
DROP_COLUMNS: Final[tuple[str, ...]] = ("", "_p_usuario")

LINEAGE_COLUMNS: Final[tuple[str, ...]] = (
    "_ingested_at",
    "_source_file",
    "_source_hash",
)

BOOL_COLUMNS: Final[tuple[str, ...]] = ("excluido", "validado")
DATETIME_COLUMNS: Final[tuple[str, ...]] = (
    "data_notificacao",
    "registro_created_at",
    "registro_updated_at",
)
NULL_STRINGS: Final[tuple[str, ...]] = (
    "",
    "NULL",
    "NULO",
    "NA",
    "N/A",
    "NONE",
    "NAN",
)


@dataclass(frozen=True)
class TransformResult:
    df: pl.DataFrame
    processed_years: list[int]


def _clean_string(col: str) -> pl.Expr:
    trimmed = pl.col(col).str.strip_chars()
    return (
        pl.when(trimmed.str.to_uppercase().is_in(NULL_STRINGS))
        .then(None)
        .otherwise(trimmed)
        .alias(col)
    )


def _to_int(col: str) -> pl.Expr:
    """``"13.0"`` → 13 (Int64). Null/inválido → 0. Negativos → 0.

    Negativos aparecem ocasionalmente no e-SUS Notifica (input
    incorreto, correções de versionamento). Como ocupação é uma
    contagem de leitos, qualquer valor abaixo de zero é semanticamente
    inválido — convertemos para 0 (mesma semântica de "não declarado").
    """
    return (
        pl.col(col)
        .cast(pl.Float64, strict=False)
        .fill_null(0)
        .cast(pl.Int64, strict=False)
        .clip(lower_bound=0)
        .alias(col)
    )


def _to_bool(col: str) -> pl.Expr:
    """``"True"`` / ``"False"`` string → Bool. None preservado."""
    up = pl.col(col).str.to_uppercase()
    return (
        pl.when(up == "TRUE")
        .then(True)
        .when(up == "FALSE")
        .then(False)
        .otherwise(None)
        .alias(col)
    )


def _to_datetime_utc(col: str) -> pl.Expr:
    """ISO 8601 string → Datetime(us, UTC). Inválido → None."""
    return (
        pl.col(col)
        .str.to_datetime(time_unit="us", time_zone="UTC", strict=False)
        .alias(col)
    )


def _staging_partition(extraction_date: date) -> Path:
    from susforge.config import get_settings  # local — evita ciclo

    return (
        get_settings().data_dir
        / "staging"
        / "covid-ocupacao"
        / "leitos"
        / extraction_date.isoformat()
    )


def transform(extraction_date: date) -> TransformResult:
    """Lê os parquets COVID, consolida, limpa, valida.

    Args:
        extraction_date: Data da partição Bronze a processar.

    Returns:
        ``TransformResult`` com DF Silver validado e a lista de anos
        processados (esperado: ``[2020, 2021, 2022]``).
    """
    partition = _staging_partition(extraction_date)
    if not partition.exists():
        raise FileNotFoundError(f"Partição Silver ausente: {partition}")

    files = sorted(partition.glob("covid_ocupacao_*.parquet"))
    if not files:
        raise FileNotFoundError(f"Nenhum parquet em {partition}")

    frames: list[pl.DataFrame] = []
    processed: list[int] = []
    for f in files:
        year = int(f.stem.rsplit("_", 1)[1])
        df_year = pl.read_parquet(f)
        # Descarta lixo
        drop = [c for c in DROP_COLUMNS if c in df_year.columns]
        if drop:
            df_year = df_year.drop(drop)
        # Renomeia para snake_case
        renames = {k: v for k, v in COLUMN_RENAMES.items() if k in df_year.columns}
        df_year = df_year.rename(renames)
        # Garante presença das colunas conhecidas + linhagem
        keep = [
            c
            for c in df_year.columns
            if c in set(COLUMN_RENAMES.values()) | set(LINEAGE_COLUMNS)
        ]
        df_year = df_year.select(keep)
        frames.append(df_year)
        processed.append(year)
        logger.info(
            "  %d ✓ %d linhas, %d cols", year, df_year.height, df_year.width
        )

    df = pl.concat(frames, how="diagonal_relaxed")
    logger.info("Consolidado: %d linhas × %d cols", df.height, df.width)

    # ---- Tipagem ----
    df = df.with_columns([_to_datetime_utc(c) for c in DATETIME_COLUMNS])
    df = df.with_columns([_to_int(c) for c in QUANTITATIVE_COLUMNS])
    df = df.with_columns([_to_bool(c) for c in BOOL_COLUMNS])

    # ---- String cleanup (loc + metadados) ----
    string_cols_to_clean = (
        "estado_notificacao",
        "municipio_notificacao",
        "estado",
        "municipio",
        "origem",
    )
    df = df.with_columns([_clean_string(c) for c in string_cols_to_clean])

    # ---- cnes: trim + lpad 7 (consistência com gold) ----
    df = df.with_columns(
        pl.col("cnes").str.strip_chars().str.zfill(7).alias("cnes")
    )

    # ---- Filtro: linhas sem identidade são lixo ----
    before = df.height
    df = df.filter(
        pl.col("id_registro").is_not_null()
        & pl.col("cnes").is_not_null()
        & pl.col("data_notificacao").is_not_null()
    )
    if df.height < before:
        logger.warning(
            "Removidas %d linhas sem identidade (id/cnes/data)",
            before - df.height,
        )

    # ---- Deriva ano_referencia ----
    df = df.with_columns(
        pl.col("data_notificacao").dt.year().cast(pl.Int64).alias("ano_referencia")
    )

    # ---- Dedup defensivo por id_registro ----
    before = df.height
    df = df.unique(subset=["id_registro"], keep="last", maintain_order=True)
    if df.height < before:
        logger.info(
            "Dedup: removidas %d duplicatas (id_registro)", before - df.height
        )

    # ---- Linhagem Silver ----
    df = df.with_columns(
        pl.lit(datetime.now(tz=timezone.utc)).alias("_cleansed_at"),
        pl.lit(extraction_date).alias("_extraction_date"),
    )

    # ---- Ordem canônica + validação ----
    df = df.select(list(SILVER_COLUMN_ORDER))
    logger.info(
        "Validando contra OcupacaoCovidSchema (%d × %d)…", df.height, df.width
    )
    OcupacaoCovidSchema.validate(df, lazy=False)
    logger.info("✓ schema validado")

    return TransformResult(df=df, processed_years=processed)


__all__ = ["TransformResult", "transform"]
