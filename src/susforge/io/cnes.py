"""Extrator Bronze do CNES — Cadastro Nacional de Estabelecimentos de Saúde.

Padrão Ouro de ingestão Bronze (referência para os demais datasets):

    * Imutabilidade: cada extração vive em sua própria partição de data
      ``<root>/<YYYY-MM-DD>/``. Nada é sobrescrito.
    * Detecção de mudança: HEAD remoto compara ETag (forte) → Last-Modified.
      Sem rede, compara hash do fallback com a última extração registrada.
    * Manifesto leve: ``data/raw/cnes/estabelecimentos/manifest.json``
      guarda a última extração e o histórico, com escrita atômica.
    * Idempotência: re-execuções sem mudança remota retornam
      ``IngestResult(changed=False)`` — a DAG decide se pula a conversão.

Regra de ouro Bronze (intocável aqui):
    * NÃO renomear colunas.
    * NÃO decodificar domínios.
    * NÃO mascarar PII / endereço — dados são públicos.
    * NÃO carregar em banco — esta camada vive 100% no data lake.
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

DATASET: Final = "cnes_estabelecimentos"
SOURCE_URL: Final = (
    "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/"
    "cnes_estabelecimentos_csv.zip"
)
FALLBACK_RELATIVE: Final = (
    Path("assistencia-saude") / "cnes" / "cnes_estabelecimentos.csv"
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
    """Snapshot imutável de uma extração realizada."""

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
    """Estado persistido do dataset na raiz da pasta raw."""

    model_config = ConfigDict(extra="forbid")

    dataset: str
    source_url: str
    last_extraction: ExtractionRecord | None = None
    history: list[ExtractionRecord] = Field(default_factory=list)


class IngestResult(BaseModel):
    """Saída de ``ingest_raw`` — payload serializável p/ XCom do Airflow."""

    model_config = ConfigDict(extra="forbid")

    extraction_date: date
    csv_path: Path
    changed: bool
    etag: str | None = None
    last_modified: str | None = None
    source_hash: str
    source: Source


# =====================================================================
# Helpers de path
# =====================================================================
def _raw_root() -> Path:
    return get_settings().data_dir / "raw" / "cnes" / "estabelecimentos"


def _staging_root() -> Path:
    return get_settings().data_dir / "staging" / "cnes" / "estabelecimentos"


def _manifest_path() -> Path:
    return _raw_root() / MANIFEST_FILENAME


def _fallback_csv() -> Path:
    return get_settings().docs_dir / FALLBACK_RELATIVE


# =====================================================================
# Persistência do manifesto (atomic write)
# =====================================================================
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
    tmp.replace(path)  # atomic rename no mesmo filesystem


# =====================================================================
# I/O primitivas
# =====================================================================
def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(IO_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _head_remote(url: str) -> tuple[str | None, str | None, int | None]:
    """HEAD no recurso remoto. Retorna (etag, last_modified, content_length)."""
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
    """True se o remoto bate com a última extração (ETag tem prioridade)."""
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
            raise RuntimeError(
                f"ZIP do CNES não contém arquivo CSV: {zf.namelist()!r}"
            )
        with zf.open(members[0]) as src, target_csv.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=IO_CHUNK_BYTES)


def _unchanged_result(record: ExtractionRecord) -> IngestResult:
    """Constrói IngestResult apontando para a última extração existente."""
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

    Fluxo:
        1. HEAD no S3 → ETag / Last-Modified.
        2. Compara com ``manifest.last_extraction``. Bate? Retorna
           ``changed=False`` sem tocar em disco.
        3. Se mudou (ou se for a primeira ingestão): baixa o ZIP,
           extrai o CSV em ``<raw_root>/<YYYY-MM-DD>/`` e atualiza o
           manifesto atomicamente.
        4. Em caso de falha de rede: usa fallback local. Se o hash do
           fallback bate com a última extração, retorna ``changed=False``
           e remove a partição que tinha sido aberta.

    Returns:
        ``IngestResult`` — sempre. Quem consome (DAG) decide pular a
        conversão se ``changed=False``.
    """
    manifest = _load_manifest()
    today = date.today()

    etag: str | None = None
    last_modified: str | None = None
    content_length: int | None = None
    source: Literal["remote", "fallback"] = "remote"

    # ---- 1) HEAD remoto + match contra o manifesto ----
    try:
        etag, last_modified, content_length = _head_remote(SOURCE_URL)
        logger.info(
            "HEAD %s → etag=%s last_modified=%s content_length=%s",
            SOURCE_URL,
            etag,
            last_modified,
            content_length,
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

    # ---- 2) Aterrissa a nova partição (preservando imutabilidade) ----
    partition_dir = _raw_root() / today.isoformat()
    partition_dir.mkdir(parents=True, exist_ok=True)
    target_csv = partition_dir / f"{DATASET}.csv"
    previous = manifest.last_extraction
    created_partition = not any(partition_dir.iterdir())

    if target_csv.exists():
        # Re-run no mesmo dia: respeita o que já está em disco.
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
        # Hash da fonte antes de copiar — evita I/O quando inalterado.
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

    # ---- 3) Idempotência por hash (caso remoto baixou conteúdo igual) ----
    if previous is not None and previous.source_hash == source_hash:
        logger.info(
            "Conteúdo igual ao processado em %s (hash bate) — preservando %s",
            previous.extraction_date,
            target_csv,
        )
        return _unchanged_result(previous)

    # ---- 4) Registra nova extração no manifesto (atomic) ----
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
    """Converte o CSV da extração corrente em Parquet particionado.

    Saída: ``data/staging/cnes/estabelecimentos/<YYYY-MM-DD>/<DATASET>.parquet``.

    Bronze NÃO tipa: todas as colunas saem como ``String`` (Polars
    ``infer_schema_length=0``). Apenas as 3 colunas técnicas de
    linhagem são tipadas (datetime + 2 strings).
    """
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
        "Gravando Parquet %s (linhas=%d, colunas=%d, compressão=%s)",
        target_parquet,
        df.height,
        df.width,
        PARQUET_COMPRESSION,
    )
    df.write_parquet(target_parquet, compression=PARQUET_COMPRESSION)

    # Reabre o manifesto e anota o parquet_path da última extração
    manifest = _load_manifest()
    if (
        manifest.last_extraction is not None
        and manifest.last_extraction.extraction_date == result.extraction_date
    ):
        manifest.last_extraction.parquet_path = str(target_parquet)
        _save_manifest(manifest)

    return target_parquet


def latest_extraction_date() -> date:
    """Data da última extração registrada — para downstream (Silver/Gold)."""
    manifest = _load_manifest()
    if manifest.last_extraction is None:
        raise RuntimeError(
            "Nenhuma extração CNES disponível — rode 'bronze_cnes_estabelecimentos'"
        )
    return manifest.last_extraction.extraction_date


def latest_parquet_path() -> Path:
    """Path do Parquet mais recente — para downstream (Silver/Gold)."""
    manifest = _load_manifest()
    if (
        manifest.last_extraction is None
        or manifest.last_extraction.parquet_path is None
    ):
        raise RuntimeError(
            "Parquet CNES ausente — rode 'bronze_cnes_estabelecimentos' "
            "ou aguarde a conversão concluir"
        )
    return Path(manifest.last_extraction.parquet_path)


__all__ = [
    "DATASET",
    "SOURCE_URL",
    "ExtractionRecord",
    "IngestResult",
    "Manifest",
    "convert_to_parquet",
    "ingest_raw",
    "latest_extraction_date",
    "latest_parquet_path",
]
