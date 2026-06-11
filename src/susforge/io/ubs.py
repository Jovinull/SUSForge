"""Extrator Bronze de UBS — Unidades Básicas de Saúde cadastradas no CNES.

Origem oficial (CKAN do Ministério da Saúde):

    https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/
        Unidades_Basicas_Saude-UBS_csv.zip

Subset do CNES focado em UBS (~48k registros, 8 colunas).
Esquema: CNES, UF, IBGE, NOME, LOGRADOURO, BAIRRO, LATITUDE, LONGITUDE.

Padrão Ouro Bronze (idêntico ao ``susforge.io.cnes`` — arquivo único):
    * Particionamento por data de extração: ``<root>/<YYYY-MM-DD>/``.
    * Detecção de mudança via HEAD (ETag → Last-Modified).
    * Manifesto leve com ``last_extraction`` e ``history[]``.
    * Imutabilidade: nunca sobrescreve nem deleta partição existente.

Regra de ouro Bronze (intocável):
    * NÃO renomear colunas, NÃO tipar, NÃO decodificar, NÃO mascarar.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Final, Literal

import polars as pl
import requests
from pydantic import BaseModel, ConfigDict, Field

from susforge.config import get_settings

logger = logging.getLogger(__name__)

DATASET: Final = "ubs"
SOURCE_URL: Final = (
    "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/"
    "Unidades_Basicas_Saude-UBS_csv.zip"
)
FALLBACK_RELATIVE: Final = (
    Path("assistencia-saude") / "ubs" / "Unidades_Basicas_Saude-UBS.csv"
)

CSV_SEPARATOR: Final = ";"
CSV_ENCODING: Final = "latin-1"
HTTP_TIMEOUT_S: Final = 600
IO_CHUNK_BYTES: Final = 1 << 20  # 1 MiB
PARQUET_COMPRESSION: Final = "zstd"
MANIFEST_FILENAME: Final = "manifest.json"

Source = Literal["remote", "fallback", "unchanged"]


# =====================================================================
# Modelos
# =====================================================================
class ExtractionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    extraction_date: date
    ingested_at: datetime
    etag: str | None = None
    last_modified: str | None = None
    remote_content_length: int | None = None
    source_hash: str
    source: Literal["remote", "fallback"]
    raw_path: str
    parquet_path: str | None = None


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset: str
    source_url: str
    last_extraction: ExtractionRecord | None = None
    history: list[ExtractionRecord] = Field(default_factory=list)


class IngestResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    extraction_date: date
    csv_path: Path
    changed: bool
    etag: str | None = None
    last_modified: str | None = None
    source_hash: str
    source: Source


# =====================================================================
# Helpers
# =====================================================================
def _raw_root() -> Path:
    return get_settings().data_dir / "raw" / "ubs" / "unidades"


def _staging_root() -> Path:
    return get_settings().data_dir / "staging" / "ubs" / "unidades"


def _manifest_path() -> Path:
    return _raw_root() / MANIFEST_FILENAME


def _fallback_csv() -> Path:
    return get_settings().docs_dir / FALLBACK_RELATIVE


def _load_manifest() -> Manifest:
    path = _manifest_path()
    if not path.exists():
        return Manifest(dataset=DATASET, source_url=SOURCE_URL)
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


def _matches_last(
    last: ExtractionRecord | None,
    etag: str | None,
    last_modified: str | None,
) -> bool:
    if last is None:
        return False
    if etag and last.etag and etag == last.etag:
        return True
    if last_modified and last.last_modified and last_modified == last.last_modified:
        return True
    return False


def _download_zip(url: str, target_zip: Path) -> None:
    logger.info("Baixando %s", url)
    with requests.get(url, stream=True, timeout=HTTP_TIMEOUT_S) as response:
        response.raise_for_status()
        with target_zip.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=IO_CHUNK_BYTES):
                fh.write(chunk)


def _extract_first_csv(zip_path: Path, target_csv: Path) -> None:
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not members:
            raise RuntimeError(f"ZIP sem CSV: {zf.namelist()!r}")
        with zf.open(members[0]) as src, target_csv.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=IO_CHUNK_BYTES)


def _unchanged_result(record: ExtractionRecord) -> IngestResult:
    return IngestResult(
        extraction_date=record.extraction_date,
        csv_path=Path(record.raw_path),
        changed=False,
        etag=record.etag,
        last_modified=record.last_modified,
        source_hash=record.source_hash,
        source="unchanged",
    )


# =====================================================================
# API pública
# =====================================================================
def ingest_raw() -> IngestResult:
    """Aterrissa CSV bruto particionado por data de extração.

    Vide ``susforge.io.cnes.ingest_raw`` — mesma semântica.
    """
    manifest = _load_manifest()
    today = date.today()

    etag: str | None = None
    last_modified: str | None = None
    content_length: int | None = None
    source: Literal["remote", "fallback"] = "remote"

    try:
        etag, last_modified, content_length = _head_remote(SOURCE_URL)
        logger.info(
            "HEAD %s → etag=%s last_modified=%s",
            SOURCE_URL,
            etag,
            last_modified,
        )
        if _matches_last(manifest.last_extraction, etag, last_modified):
            assert manifest.last_extraction is not None
            logger.info(
                "Dataset inalterado (match com %s) — pulando download",
                manifest.last_extraction.extraction_date,
            )
            return _unchanged_result(manifest.last_extraction)
    except requests.RequestException as exc:
        logger.warning("HEAD remoto falhou (%s) — usando fallback offline", exc)
        source = "fallback"

    partition_dir = _raw_root() / today.isoformat()
    partition_dir.mkdir(parents=True, exist_ok=True)
    target_csv = partition_dir / f"{DATASET}.csv"
    previous = manifest.last_extraction
    created_partition = not any(partition_dir.iterdir())

    if target_csv.exists():
        source_hash = _sha256_of(target_csv)
    elif source == "remote":
        zip_path = partition_dir / f"{DATASET}.zip"
        try:
            _download_zip(SOURCE_URL, zip_path)
            _extract_first_csv(zip_path, target_csv)
        finally:
            if zip_path.exists():
                zip_path.unlink()
        source_hash = _sha256_of(target_csv)
    else:
        fallback = _fallback_csv()
        if not fallback.exists():
            raise FileNotFoundError(
                f"HEAD remoto falhou e fallback ausente: {fallback}"
            )
        source_hash = _sha256_of(fallback)
        if previous is not None and previous.source_hash == source_hash:
            if created_partition:
                try:
                    partition_dir.rmdir()
                except OSError:
                    pass
            logger.info(
                "Fallback igual à última extração %s — pulando cópia",
                previous.extraction_date,
            )
            return _unchanged_result(previous)
        shutil.copy2(fallback, target_csv)

    if previous is not None and previous.source_hash == source_hash:
        logger.info(
            "Conteúdo igual ao processado em %s — preservando %s",
            previous.extraction_date,
            target_csv,
        )
        return _unchanged_result(previous)

    new_record = ExtractionRecord(
        extraction_date=today,
        ingested_at=datetime.now(tz=timezone.utc),
        etag=etag,
        last_modified=last_modified,
        remote_content_length=content_length,
        source_hash=source_hash,
        source=source,
        raw_path=str(target_csv),
    )
    if manifest.last_extraction is not None:
        manifest.history.append(manifest.last_extraction)
    manifest.last_extraction = new_record
    _save_manifest(manifest)

    logger.info(
        "Nova extração registrada: date=%s source=%s hash=%s…",
        today.isoformat(),
        source,
        source_hash[:16],
    )
    return IngestResult(
        extraction_date=today,
        csv_path=target_csv,
        changed=True,
        etag=etag,
        last_modified=last_modified,
        source_hash=source_hash,
        source=source,
    )


def convert_to_parquet(result: IngestResult) -> Path:
    """Lê CSV bruto com Polars (tudo String) e grava Parquet com linhagem."""
    if not result.csv_path.exists():
        raise FileNotFoundError(f"CSV não encontrado: {result.csv_path}")

    partition_dir = _staging_root() / result.extraction_date.isoformat()
    partition_dir.mkdir(parents=True, exist_ok=True)
    target_parquet = partition_dir / f"{DATASET}.parquet"

    logger.info("Lendo CSV %s (encoding=%s)", result.csv_path, CSV_ENCODING)
    df = pl.read_csv(
        result.csv_path,
        separator=CSV_SEPARATOR,
        encoding=CSV_ENCODING,
        infer_schema_length=0,
        truncate_ragged_lines=False,
        ignore_errors=False,
        low_memory=False,
    )

    df = df.with_columns(
        pl.lit(datetime.now(tz=timezone.utc)).alias("_ingested_at"),
        pl.lit(result.csv_path.name).alias("_source_file"),
        pl.lit(result.source_hash).alias("_source_hash"),
    )

    logger.info(
        "Gravando Parquet %s (linhas=%d, colunas=%d)",
        target_parquet,
        df.height,
        df.width,
    )
    df.write_parquet(target_parquet, compression=PARQUET_COMPRESSION)

    manifest = _load_manifest()
    if (
        manifest.last_extraction is not None
        and manifest.last_extraction.extraction_date == result.extraction_date
    ):
        manifest.last_extraction.parquet_path = str(target_parquet)
        _save_manifest(manifest)

    return target_parquet


def latest_extraction_date() -> date:
    """Data da última extração — para downstream Silver/Gold."""
    manifest = _load_manifest()
    if manifest.last_extraction is None:
        raise RuntimeError("Nenhuma extração UBS disponível — rode 'bronze_ubs'")
    return manifest.last_extraction.extraction_date


__all__ = [
    "DATASET",
    "SOURCE_URL",
    "ExtractionRecord",
    "IngestResult",
    "Manifest",
    "convert_to_parquet",
    "ingest_raw",
    "latest_extraction_date",
]
