"""Extrator Bronze do CNES — Cadastro Nacional de Estabelecimentos de Saúde.

Origem oficial (DEMAS / CKAN do Ministério da Saúde):

    https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/cnes_estabelecimentos_csv.zip

Comportamento:
    1. ``ingest_raw()`` baixa o ZIP do S3 público, extrai o CSV e
       grava em ``data/raw/cnes/estabelecimentos/cnes_estabelecimentos.csv``.
       Em caso de falha de rede (timeout, 5xx, DNS), recorre a um
       fallback local em ``docs/assistencia-saude/cnes/`` para garantir
       reprodutibilidade offline.
    2. ``convert_to_parquet()`` lê o CSV bruto com Polars (tudo como
       ``String`` — Bronze NÃO tipa), anexa metadados técnicos de
       linhagem (``_ingested_at``, ``_source_file``, ``_source_hash``)
       e grava Parquet comprimido (zstd) em
       ``data/staging/cnes/estabelecimentos/cnes_estabelecimentos.parquet``.

Regra de ouro Bronze (intocável aqui):
    * NÃO renomear colunas (preserva contrato com o produtor).
    * NÃO decodificar domínios (CO_TURNO_ATENDIMENTO etc. ficam crus).
    * NÃO mascarar PII / endereço — dados são públicos; a Silver decide
      o que expor e como.
    * NÃO carregar em banco — esta camada vive 100% no data lake.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import polars as pl
import requests

from susforge.config import get_settings

logger = logging.getLogger(__name__)

DATASET: Final = "cnes_estabelecimentos"
SOURCE_URL: Final = (
    "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/"
    "cnes_estabelecimentos_csv.zip"
)
FALLBACK_RELATIVE: Final = (
    Path("assistencia-saude") / "cnes" / "cnes_estabelecimentos.csv"
)

CSV_SEPARATOR: Final = ";"
# DATASUS e CKAN do MS entregam o CNES em latin-1 (ISO-8859-1). O conteúdo
# costuma estar normalizado para maiúsculas sem acentos, mas usamos latin-1
# por segurança — utf-8 quebraria em bytes 0x80–0xFF eventuais.
CSV_ENCODING: Final = "latin-1"
HTTP_TIMEOUT_S: Final = 600
IO_CHUNK_BYTES: Final = 1 << 20  # 1 MiB

PARQUET_COMPRESSION: Final = "zstd"


def _raw_dir() -> Path:
    return get_settings().data_dir / "raw" / "cnes" / "estabelecimentos"


def _staging_dir() -> Path:
    return get_settings().data_dir / "staging" / "cnes" / "estabelecimentos"


def _fallback_csv() -> Path:
    return get_settings().docs_dir / FALLBACK_RELATIVE


def _sha256_of(path: Path) -> str:
    """SHA256 streaming — não carrega o arquivo inteiro em memória."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(IO_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_zip(url: str, target_zip: Path) -> None:
    logger.info("Baixando CNES de %s", url)
    with requests.get(url, stream=True, timeout=HTTP_TIMEOUT_S) as response:
        response.raise_for_status()
        with target_zip.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=IO_CHUNK_BYTES):
                fh.write(chunk)


def _extract_first_csv(zip_path: Path, target_csv: Path) -> None:
    with zipfile.ZipFile(zip_path) as zf:
        members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
        if not members:
            raise RuntimeError(
                f"ZIP do CNES não contém arquivo CSV: {zf.namelist()!r}"
            )
        with zf.open(members[0]) as src, target_csv.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=IO_CHUNK_BYTES)


def ingest_raw(*, force_download: bool = False) -> Path:
    """Aterrissa o CSV bruto do CNES em ``data/raw/cnes/estabelecimentos/``.

    Tenta o S3 público primeiro; em caso de falha de rede, usa o
    fallback local em ``docs/assistencia-saude/cnes/``.

    Args:
        force_download: se True, ignora o cache local e rebaixa.

    Returns:
        Path absoluto do CSV bruto aterrissado.
    """
    raw_dir = _raw_dir()
    raw_dir.mkdir(parents=True, exist_ok=True)
    target_csv = raw_dir / f"{DATASET}.csv"

    if target_csv.exists() and not force_download:
        logger.info("CSV já presente em %s — pulando download", target_csv)
        return target_csv

    zip_path = raw_dir / f"{DATASET}.zip"
    try:
        _download_zip(SOURCE_URL, zip_path)
        _extract_first_csv(zip_path, target_csv)
    except (requests.RequestException, RuntimeError, zipfile.BadZipFile) as exc:
        fallback = _fallback_csv()
        logger.warning(
            "Falha ao baixar do S3 (%s); usando fallback local em %s",
            exc,
            fallback,
        )
        if not fallback.exists():
            raise FileNotFoundError(
                f"Download falhou e fallback local ausente: {fallback}"
            ) from exc
        shutil.copy2(fallback, target_csv)
    finally:
        if zip_path.exists():
            zip_path.unlink()

    logger.info("CSV bruto aterrissado em %s", target_csv)
    return target_csv


def convert_to_parquet(raw_csv: Path) -> Path:
    """Lê o CSV bruto com Polars e grava Parquet com linhagem.

    Bronze é "schema-on-read": todas as colunas saem como ``String`` —
    a tipagem é responsabilidade da Silver. Apenas as 3 colunas técnicas
    de linhagem são tipadas (timestamp + 2 strings).

    Args:
        raw_csv: Caminho do CSV bruto retornado por ``ingest_raw``.

    Returns:
        Path absoluto do Parquet gerado.
    """
    if not raw_csv.exists():
        raise FileNotFoundError(f"CSV bruto não encontrado: {raw_csv}")

    staging_dir = _staging_dir()
    staging_dir.mkdir(parents=True, exist_ok=True)
    target_parquet = staging_dir / f"{DATASET}.parquet"

    logger.info("Lendo CSV bruto %s (encoding=%s)", raw_csv, CSV_ENCODING)
    df = pl.read_csv(
        raw_csv,
        separator=CSV_SEPARATOR,
        encoding=CSV_ENCODING,
        infer_schema_length=0,  # tudo String — Silver tipa
        truncate_ragged_lines=False,
        ignore_errors=False,
        low_memory=False,
    )

    ingested_at = datetime.now(tz=timezone.utc)
    source_hash = _sha256_of(raw_csv)

    df = df.with_columns(
        pl.lit(ingested_at).alias("_ingested_at"),
        pl.lit(raw_csv.name).alias("_source_file"),
        pl.lit(source_hash).alias("_source_hash"),
    )

    logger.info(
        "Gravando Parquet em %s (linhas=%d, colunas=%d, compressão=%s)",
        target_parquet,
        df.height,
        df.width,
        PARQUET_COMPRESSION,
    )
    df.write_parquet(target_parquet, compression=PARQUET_COMPRESSION)
    return target_parquet


__all__ = [
    "DATASET",
    "SOURCE_URL",
    "ingest_raw",
    "convert_to_parquet",
]
