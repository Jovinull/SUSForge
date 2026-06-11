"""Extrator Bronze de Registro de Ocupação Hospitalar COVID-19.

Origem oficial (e-SUS Notifica — módulo Internações SUS, via CKAN):

    https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/LEITOS/
        <PUBLICATION_PATH>/esus-vepi.LeitoOcupacao_{YYYY}.csv

Dataset **descontinuado** — só existem os anos **2020, 2021, 2022**.

ATENÇÃO — URL volátil
---------------------
O CKAN do MS publica esses CSVs sob um path datado
(``LEITOS/<YYYY-MM-DD>/...``) que muda quando o MS republica. Esta
constante fica em ``PUBLICATION_PATH``; quando o MS atualizar o
dataset, baste atualizar essa string. O fallback offline em ``docs/``
cobre o caso de a URL ter mudado e ainda não termos detectado.

Padrão Ouro Bronze (multi-arquivo, idêntico ao ``hospitais_leitos``,
adaptado para CSV direto — sem ZIP intermediário):

    * Particionamento por data de extração.
    * Detecção de mudança POR ANO via HEAD (ETag/Last-Modified).
    * Manifest com ``last_extraction.years[year]``.
    * Fallback por-ano: se um ano falhar no S3, cai para ``docs/``.
    * Schema diferente entre anos? Bronze NÃO unifica. Cada parquet
      preserva o schema da fonte (a partir de 2022 o MS adicionou
      colunas novas — fica como veio).

Regra de ouro Bronze (intocável):
    * NÃO renomear colunas, NÃO tipar, NÃO decodificar, NÃO mascarar.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final, Literal

import polars as pl
import requests
from pydantic import BaseModel, ConfigDict, Field

from susforge.config import get_settings

logger = logging.getLogger(__name__)

DATASET: Final = "covid_ocupacao"
BASE_URL: Final = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/LEITOS"
# Atualizar quando o MS republicar (ver sobre.txt do dataset).
PUBLICATION_PATH: Final = "2026-06-04"
AVAILABLE_YEARS: Final = (2020, 2021, 2022)  # dataset descontinuado
FALLBACK_RELATIVE: Final = (
    Path("assistencia-saude") / "Registro de Ocupação Hospitalar COVID-19"
)

CSV_SEPARATOR: Final = ","
CSV_ENCODING: Final = "utf-8"  # e-SUS Notifica usa UTF-8 (validado)
HTTP_TIMEOUT_S: Final = 600
IO_CHUNK_BYTES: Final = 1 << 20  # 1 MiB
PARQUET_COMPRESSION: Final = "zstd"
MANIFEST_FILENAME: Final = "manifest.json"


def _source_url(year: int) -> str:
    return f"{BASE_URL}/{PUBLICATION_PATH}/esus-vepi.LeitoOcupacao_{year}.csv"


def _csv_filename(year: int) -> str:
    return f"esus-vepi.LeitoOcupacao_{year}.csv"


def _parquet_filename(year: int) -> str:
    return f"covid_ocupacao_{year}.parquet"


# =====================================================================
# Modelos
# =====================================================================
class YearRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    year: int
    etag: str | None = None
    last_modified: str | None = None
    remote_content_length: int | None = None
    source_hash: str
    source: Literal["remote", "fallback"]
    raw_path: str
    parquet_path: str | None = None


class ExtractionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    extraction_date: date
    ingested_at: datetime
    publication_path: str
    years: dict[int, YearRecord] = Field(default_factory=dict)


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset: str
    base_url: str
    last_extraction: ExtractionRecord | None = None
    history: list[ExtractionRecord] = Field(default_factory=list)


class IngestResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    extraction_date: date
    changed: bool
    new_records: list[YearRecord] = Field(default_factory=list)
    unchanged_years: list[int] = Field(default_factory=list)
    failed_years: list[int] = Field(default_factory=list)


# =====================================================================
# Helpers
# =====================================================================
def _raw_root() -> Path:
    return get_settings().data_dir / "raw" / "covid-ocupacao" / "leitos"


def _staging_root() -> Path:
    return get_settings().data_dir / "staging" / "covid-ocupacao" / "leitos"


def _manifest_path() -> Path:
    return _raw_root() / MANIFEST_FILENAME


def _fallback_csv_for_year(year: int) -> Path:
    return get_settings().docs_dir / FALLBACK_RELATIVE / _csv_filename(year)


def _load_manifest() -> Manifest:
    path = _manifest_path()
    if not path.exists():
        return Manifest(dataset=DATASET, base_url=BASE_URL)
    return Manifest.model_validate_json(path.read_text(encoding="utf-8"))


def _save_manifest(manifest: Manifest) -> None:
    path = _manifest_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    tmp.replace(path)


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(IO_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _head_remote(url: str) -> tuple[str | None, str | None, int | None]:
    response = requests.head(url, timeout=HTTP_TIMEOUT_S, allow_redirects=True)
    response.raise_for_status()
    cl = response.headers.get("Content-Length")
    return (
        response.headers.get("ETag"),
        response.headers.get("Last-Modified"),
        int(cl) if cl else None,
    )


def _etag_matches(
    previous: YearRecord | None,
    etag: str | None,
    last_modified: str | None,
) -> bool:
    if previous is None:
        return False
    if etag and previous.etag and etag == previous.etag:
        return True
    if (
        last_modified
        and previous.last_modified
        and last_modified == previous.last_modified
    ):
        return True
    return False


def _download_csv(url: str, target_csv: Path) -> None:
    """Baixa CSV direto (sem ZIP intermediário)."""
    logger.info("Baixando %s", url)
    with requests.get(url, stream=True, timeout=HTTP_TIMEOUT_S) as response:
        response.raise_for_status()
        with target_csv.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=IO_CHUNK_BYTES):
                fh.write(chunk)


# =====================================================================
# Processamento por ano
# =====================================================================
def _process_year_remote(
    year: int,
    previous: YearRecord | None,
    partition_dir: Path,
) -> tuple[YearRecord | None, bool]:
    url = _source_url(year)
    etag, last_modified, content_length = _head_remote(url)
    logger.info("HEAD %s → etag=%s last_modified=%s", url, etag, last_modified)

    if _etag_matches(previous, etag, last_modified):
        logger.info("  %d inalterado (ETag/Last-Modified bate)", year)
        return None, False

    target_csv = partition_dir / _csv_filename(year)
    if not target_csv.exists():
        _download_csv(url, target_csv)
    source_hash = _sha256_of(target_csv)

    if previous is not None and previous.source_hash == source_hash:
        logger.info(
            "  %d ETag mudou mas conteúdo é idêntico — atualizando manifest, "
            "preservando partição anterior",
            year,
        )
        target_csv.unlink(missing_ok=True)
        return (
            YearRecord(
                year=year,
                etag=etag,
                last_modified=last_modified,
                remote_content_length=content_length,
                source_hash=previous.source_hash,
                source="remote",
                raw_path=previous.raw_path,
                parquet_path=previous.parquet_path,
            ),
            False,
        )

    return (
        YearRecord(
            year=year,
            etag=etag,
            last_modified=last_modified,
            remote_content_length=content_length,
            source_hash=source_hash,
            source="remote",
            raw_path=str(target_csv),
        ),
        True,
    )


def _process_year_fallback(
    year: int,
    previous: YearRecord | None,
    partition_dir: Path,
) -> tuple[YearRecord | None, bool]:
    fallback = _fallback_csv_for_year(year)
    if not fallback.exists():
        raise FileNotFoundError(f"Fallback ausente: {fallback}")

    target_csv = partition_dir / _csv_filename(year)

    if target_csv.exists():
        source_hash = _sha256_of(target_csv)
    else:
        source_hash = _sha256_of(fallback)
        if previous is not None and previous.source_hash == source_hash:
            logger.info(
                "  %d fallback igual ao último processado — pulando cópia",
                year,
            )
            return None, False
        shutil.copy2(fallback, target_csv)

    if previous is not None and previous.source_hash == source_hash:
        return None, False

    return (
        YearRecord(
            year=year,
            source_hash=source_hash,
            source="fallback",
            raw_path=str(target_csv),
        ),
        True,
    )


# =====================================================================
# API pública
# =====================================================================
def ingest_raw(*, years: list[int] | None = None) -> IngestResult:
    """Aterrissa cada ano disponível em ``data/raw/.../<YYYY-MM-DD>/``.

    Tenta o S3 primeiro; qualquer falha (rede, 403, 404 por URL volátil
    desatualizada) cai automaticamente para o fallback local.
    """
    if years is None:
        years = list(AVAILABLE_YEARS)

    manifest = _load_manifest()
    today = date.today()
    partition_dir = _raw_root() / today.isoformat()
    partition_dir.mkdir(parents=True, exist_ok=True)

    previous_years: dict[int, YearRecord] = (
        manifest.last_extraction.years if manifest.last_extraction else {}
    )

    new_records: list[YearRecord] = []
    unchanged_years: list[int] = []
    failed_years: list[int] = []
    etag_only_updates: list[YearRecord] = []

    for year in years:
        previous = previous_years.get(year)
        record: YearRecord | None = None
        is_new = False

        try:
            record, is_new = _process_year_remote(year, previous, partition_dir)
        except (requests.RequestException, RuntimeError) as exc:
            logger.info(
                "  %d remoto indisponível (%s) — usando fallback local",
                year,
                exc,
            )
            try:
                record, is_new = _process_year_fallback(
                    year, previous, partition_dir
                )
            except (FileNotFoundError, OSError) as fallback_exc:
                logger.warning(
                    "  %d sem remoto e sem fallback (%s) — pulando",
                    year,
                    fallback_exc,
                )
                failed_years.append(year)
                continue

        if record is None:
            unchanged_years.append(year)
        elif is_new:
            new_records.append(record)
        else:
            etag_only_updates.append(record)
            unchanged_years.append(year)

    if new_records or etag_only_updates:
        merged_years: dict[int, YearRecord] = dict(previous_years)
        for rec in new_records + etag_only_updates:
            merged_years[rec.year] = rec
        new_extraction = ExtractionRecord(
            extraction_date=today,
            ingested_at=datetime.now(tz=timezone.utc),
            publication_path=PUBLICATION_PATH,
            years=merged_years,
        )
        if manifest.last_extraction is not None:
            manifest.history.append(manifest.last_extraction)
        manifest.last_extraction = new_extraction
        _save_manifest(manifest)

    if not new_records:
        try:
            partition_dir.rmdir()
        except OSError:
            pass

    logger.info(
        "Resumo: novos=%d, inalterados=%d, falhados=%d",
        len(new_records),
        len(unchanged_years),
        len(failed_years),
    )

    return IngestResult(
        extraction_date=today,
        changed=bool(new_records),
        new_records=new_records,
        unchanged_years=unchanged_years,
        failed_years=failed_years,
    )


def convert_to_parquet(result: IngestResult) -> list[Path]:
    """Converte cada CSV novo em Parquet particionado por data."""
    if not result.new_records:
        logger.info("Nenhum ano novo para converter")
        return []

    partition_dir = _staging_root() / result.extraction_date.isoformat()
    partition_dir.mkdir(parents=True, exist_ok=True)

    targets: list[Path] = []
    for record in result.new_records:
        csv_path = Path(record.raw_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

        target_parquet = partition_dir / _parquet_filename(record.year)
        logger.info("Lendo CSV %s", csv_path)
        df = pl.read_csv(
            csv_path,
            separator=CSV_SEPARATOR,
            encoding=CSV_ENCODING,
            infer_schema_length=0,
            truncate_ragged_lines=False,
            ignore_errors=False,
            low_memory=False,
        )

        df = df.with_columns(
            pl.lit(datetime.now(tz=timezone.utc)).alias("_ingested_at"),
            pl.lit(csv_path.name).alias("_source_file"),
            pl.lit(record.source_hash).alias("_source_hash"),
        )

        logger.info(
            "Gravando Parquet %s (linhas=%d, colunas=%d)",
            target_parquet,
            df.height,
            df.width,
        )
        df.write_parquet(target_parquet, compression=PARQUET_COMPRESSION)
        targets.append(target_parquet)

    manifest = _load_manifest()
    if (
        manifest.last_extraction is not None
        and manifest.last_extraction.extraction_date == result.extraction_date
    ):
        for record, target in zip(result.new_records, targets, strict=True):
            existing = manifest.last_extraction.years.get(record.year)
            if existing is not None:
                existing.parquet_path = str(target)
        _save_manifest(manifest)

    return targets


def latest_extraction_date() -> date:
    """Data da última extração — para downstream Silver/Gold."""
    manifest = _load_manifest()
    if manifest.last_extraction is None:
        raise RuntimeError(
            "Nenhuma extração COVID-Ocupação disponível — "
            "rode 'bronze_covid_ocupacao'"
        )
    return manifest.last_extraction.extraction_date


__all__ = [
    "AVAILABLE_YEARS",
    "BASE_URL",
    "DATASET",
    "PUBLICATION_PATH",
    "ExtractionRecord",
    "IngestResult",
    "Manifest",
    "YearRecord",
    "convert_to_parquet",
    "ingest_raw",
    "latest_extraction_date",
]
