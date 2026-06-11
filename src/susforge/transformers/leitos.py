"""Transformer Silver para ``silver.leitos_anual`` — consolida 20 anos.

Drifts conhecidos do Bronze (esperados, tratados aqui):

    * **Nomenclatura 2007–2022**: nomes com **espaços e hífens**
      (``"LEITOS EXISTENTE"``, ``"UTI TOTAL - EXIST"``).
    * **Nomenclatura 2023–2024**: nomes com **underscores**
      (``LEITOS_EXISTENTES``, ``UTI_TOTAL_EXIST``).
    * **2025/2026 — CSV CORROMPIDO**: o MS mudou o separador para
      ``;`` e o Bronze (que lê com ``,``) entrega cada linha numa
      única coluna. **Pulamos esses anos com warning** até o Bronze
      ser corrigido para detectar o separador correto. O retorno
      do transformer inclui a lista de anos pulados.

Decisões:
    * Lê parquets do diretório ``data/staging/hospitais-leitos/leitos/
      <YYYY-MM-DD>/leitos_*.parquet`` (extração mais recente).
    * Para cada ano: normaliza nomes via mapping canônico; cast
      quantitativos para Int64 com ``fill_null(0)``; descarta linhas
      com ``comp`` inválido.
    * Consolida via ``pl.concat(..., how="diagonal_relaxed")`` —
      tolera ausência de colunas entre anos.
    * Dedup defensivo por ``(comp, cnes)`` mantendo a **última**
      ocorrência (proxy para "mais recente").
    * Valida com ``LeitosAnualSchema`` antes de devolver.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final

import polars as pl

from susforge.schemas.leitos import (
    QUANTITATIVE_COLUMNS,
    SILVER_COLUMN_ORDER,
    LeitosAnualSchema,
)

logger = logging.getLogger(__name__)


# Mapeia QUALQUER variante (com espaço/hífen ou underscore) para o
# canônico em snake_case lowercase. Cobertura: schemas Bronze #1 e #2.
CANON_NAME_MAP: Final[dict[str, str]] = {
    "COMP": "comp",
    "REGIAO": "regiao",
    "UF": "uf",
    "MUNICIPIO": "municipio",
    "MOTIVO DESABILITACAO": "motivo_desabilitacao",
    "MOTIVO_DESABILITACAO": "motivo_desabilitacao",
    "CNES": "cnes",
    "NOME ESTABELECIMENTO": "nome_estabelecimento",
    "NOME_ESTABELECIMENTO": "nome_estabelecimento",
    "RAZAO SOCIAL": "razao_social",
    "RAZAO_SOCIAL": "razao_social",
    "TP_GESTAO": "tp_gestao",
    "CO_TIPO_UNIDADE": "co_tipo_unidade",
    "DS_TIPO_UNIDADE": "ds_tipo_unidade",
    "NATUREZA_JURIDICA": "natureza_juridica",
    "DESC_NATUREZA_JURIDICA": "desc_natureza_juridica",
    "NO_LOGRADOURO": "no_logradouro",
    "NU_ENDERECO": "nu_endereco",
    "NO_COMPLEMENTO": "no_complemento",
    "NO_BAIRRO": "no_bairro",
    "CO_CEP": "co_cep",
    "NU_TELEFONE": "nu_telefone",
    "NO_EMAIL": "no_email",
    "LEITOS EXISTENTE": "leitos_existentes",
    "LEITOS_EXISTENTES": "leitos_existentes",
    "LEITOS SUS": "leitos_sus",
    "LEITOS_SUS": "leitos_sus",
    "UTI TOTAL - EXIST": "uti_total_exist",
    "UTI_TOTAL_EXIST": "uti_total_exist",
    "UTI TOTAL - SUS": "uti_total_sus",
    "UTI_TOTAL_SUS": "uti_total_sus",
    "UTI ADULTO - EXIST": "uti_adulto_exist",
    "UTI_ADULTO_EXIST": "uti_adulto_exist",
    "UTI ADULTO - SUS": "uti_adulto_sus",
    "UTI_ADULTO_SUS": "uti_adulto_sus",
    "UTI PEDIATRICO - EXIST": "uti_pediatrico_exist",
    "UTI_PEDIATRICO_EXIST": "uti_pediatrico_exist",
    "UTI PEDIATRICO - SUS": "uti_pediatrico_sus",
    "UTI_PEDIATRICO_SUS": "uti_pediatrico_sus",
    "UTI NEONATAL - EXIST": "uti_neonatal_exist",
    "UTI_NEONATAL_EXIST": "uti_neonatal_exist",
    "UTI NEONATAL - SUS": "uti_neonatal_sus",
    "UTI_NEONATAL_SUS": "uti_neonatal_sus",
    "UTI QUEIMADO - EXIST": "uti_queimado_exist",
    "UTI_QUEIMADO_EXIST": "uti_queimado_exist",
    "UTI QUEIMADO - SUS": "uti_queimado_sus",
    "UTI_QUEIMADO_SUS": "uti_queimado_sus",
    "UTI CORONARIANA - EXIST": "uti_coronariana_exist",
    "UTI_CORONARIANA_EXIST": "uti_coronariana_exist",
    "UTI CORONARIANA - SUS": "uti_coronariana_sus",
    "UTI_CORONARIANA_SUS": "uti_coronariana_sus",
}

# Linhagem do Bronze (não renomeada).
LINEAGE_COLUMNS: Final[tuple[str, ...]] = (
    "_ingested_at",
    "_source_file",
    "_source_hash",
)

YEAR_RE = re.compile(r"leitos_(\d{4})\.parquet$")


@dataclass(frozen=True)
class TransformResult:
    df: pl.DataFrame
    processed_years: list[int]
    skipped_years: list[int]


def _is_corrupted(df: pl.DataFrame) -> bool:
    """Detecta parquets com schema quebrado pelo Bronze (1 coluna gigante)."""
    return "COMP" not in df.columns


def _normalize_one_year(df: pl.DataFrame) -> pl.DataFrame:
    """Renomeia bronze→canônico e descarta colunas desconhecidas/silver."""
    renames = {old: new for old, new in CANON_NAME_MAP.items() if old in df.columns}
    df = df.rename(renames)
    # Preserva apenas as colunas canônicas + linhagem (drop do que sobrou).
    keep = [
        c
        for c in df.columns
        if c in set(CANON_NAME_MAP.values()) | set(LINEAGE_COLUMNS)
    ]
    return df.select(keep)


def _ensure_quantitatives(df: pl.DataFrame) -> pl.DataFrame:
    """Garante todas as colunas FATO presentes como Int64 (default 0)."""
    add: list[pl.Expr] = []
    for col in QUANTITATIVE_COLUMNS:
        if col not in df.columns:
            add.append(pl.lit(0, dtype=pl.Int64).alias(col))
    if add:
        df = df.with_columns(add)
    return df.with_columns(
        [
            pl.col(col)
            .cast(pl.Int64, strict=False)
            .fill_null(0)
            .alias(col)
            for col in QUANTITATIVE_COLUMNS
        ]
    )


def _add_ano_referencia(df: pl.DataFrame) -> pl.DataFrame:
    """Deriva ano_referencia de ``comp`` (primeiros 4 caracteres → Int32)."""
    return df.with_columns(
        pl.col("comp")
        .str.slice(0, 4)
        .cast(pl.Int64, strict=False)
        .alias("ano_referencia")
    )


def _staging_partition(extraction_date: date) -> Path:
    """Resolve a partição Bronze→Silver para a data de extração."""
    from susforge.config import get_settings  # local import — evita ciclo

    return (
        get_settings().data_dir
        / "staging"
        / "hospitais-leitos"
        / "leitos"
        / extraction_date.isoformat()
    )


def transform(extraction_date: date) -> TransformResult:
    """Lê todos os parquets da partição, normaliza, consolida e valida.

    Args:
        extraction_date: Data da partição Bronze a processar.

    Returns:
        ``TransformResult`` com o DataFrame Silver validado e a lista
        de anos efetivamente processados / pulados (por corrupção).
    """
    partition = _staging_partition(extraction_date)
    if not partition.exists():
        raise FileNotFoundError(f"Partição Silver ausente: {partition}")

    files = sorted(partition.glob("leitos_*.parquet"))
    if not files:
        raise FileNotFoundError(f"Nenhum parquet em {partition}")

    logger.info("Encontrados %d parquets em %s", len(files), partition)

    frames: list[pl.DataFrame] = []
    processed: list[int] = []
    skipped: list[int] = []

    for f in files:
        match = YEAR_RE.search(f.name)
        if not match:
            logger.warning("Ignorando arquivo fora do padrão: %s", f.name)
            continue
        year = int(match.group(1))

        df_year = pl.read_parquet(f)
        if _is_corrupted(df_year):
            logger.warning(
                "  %d ⚠ parquet corrompido (Bronze separator bug) — pulando",
                year,
            )
            skipped.append(year)
            continue

        df_year = _normalize_one_year(df_year)
        df_year = _ensure_quantitatives(df_year)
        frames.append(df_year)
        processed.append(year)
        logger.info("  %d ✓ %d linhas, %d cols", year, df_year.height, df_year.width)

    if not frames:
        raise RuntimeError("Nenhum parquet processável — toda a série está corrompida")

    # Consolida — tolera ausência de colunas entre anos antigos vs novos.
    df = pl.concat(frames, how="diagonal_relaxed")
    logger.info("Consolidado: %d linhas × %d cols", df.height, df.width)

    df = _add_ano_referencia(df)

    # Descarta linhas com comp inválido (proteção contra dados sujos).
    before = df.height
    df = df.filter(pl.col("comp").str.contains(r"^\d{6}$"))
    if df.height < before:
        logger.warning("Removidas %d linhas com comp inválido", before - df.height)

    # Dedup por (comp, cnes) — mantém a última ocorrência observada
    # (defensivo: caso o MS publique correções intra-arquivo).
    before = df.height
    df = df.unique(subset=["comp", "cnes"], keep="last", maintain_order=True)
    if df.height < before:
        logger.info("Dedup: removidas %d duplicatas (comp, cnes)", before - df.height)

    # Linhagem Silver.
    df = df.with_columns(
        pl.lit(datetime.now(tz=timezone.utc)).alias("_cleansed_at"),
        pl.lit(extraction_date).alias("_extraction_date"),
    )

    # Ordem canônica para o COPY.
    df = df.select(list(SILVER_COLUMN_ORDER))

    logger.info(
        "Validando contra LeitosAnualSchema (%d linhas, %d cols)…",
        df.height,
        df.width,
    )
    LeitosAnualSchema.validate(df, lazy=False)
    logger.info("✓ schema validado")

    return TransformResult(df=df, processed_years=processed, skipped_years=skipped)


__all__ = ["TransformResult", "transform"]
