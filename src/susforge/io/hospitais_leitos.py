"""Extrator Bronze de Hospitais e Leitos — série histórica anual.

Origem oficial (CKAN do Ministério da Saúde):

    https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos_SUS/
        Leitos_csv_{YYYY}.zip

Cada ano é um recurso S3 independente. O ano em curso é atualizado
mensalmente pelo MS; anos anteriores são essencialmente estáticos.

Padrão Ouro Bronze (idêntico ao ``susforge.io.cnes``, adaptado para
multi-arquivo):

    * Particionamento por data de extração: ``<root>/<YYYY-MM-DD>/``.
      Dentro da partição, um CSV/Parquet por ano.
    * Detecção de mudança POR ANO via HEAD (ETag → Last-Modified).
    * Manifesto leve com ``last_extraction.years[year]`` rastreando
      ETag, Last-Modified, content_length, source_hash e paths.
    * Fallback offline POR ANO: cada ano tenta o S3; em qualquer
      falha (rede, 403, ZIP corrompido) cai automaticamente para o
      CSV local em ``docs/``. Cenário observado em produção: o CKAN
      do MS só publica os 1–2 anos mais recentes; os anteriores
      voltam 403 e devem vir do fallback.
    * Idempotência: ETag bate → skip; conteúdo igual mas ETag mudou →
      atualiza ETag no manifest sem regravar Parquet.

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

DATASET: Final = "hospitais_leitos"
BASE_URL: Final = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos_SUS"
FIRST_YEAR: Final = 2007
FALLBACK_RELATIVE: Final = Path("assistencia-saude") / "Hospitais e Leitos"

CSV_SEPARATOR: Final = ","
CSV_ENCODING: Final = "latin-1"
HTTP_TIMEOUT_S: Final = 600
IO_CHUNK_BYTES: Final = 1 << 20  # 1 MiB
PARQUET_COMPRESSION: Final = "zstd"
MANIFEST_FILENAME: Final = "manifest.json"


def _source_url(year: int) -> str:
    return f"{BASE_URL}/Leitos_csv_{year}.zip"


def _csv_filename(year: int) -> str:
    return f"Leitos_{year}.csv"


def _parquet_filename(year: int) -> str:
    return f"leitos_{year}.parquet"


# =====================================================================
# Modelos
# =====================================================================
class YearRecord(BaseModel):
    """Snapshot de um ano específico aterrissado."""

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
    """Snapshot de uma extração inteira (todos os anos de uma rodada)."""

    model_config = ConfigDict(extra="forbid")

    extraction_date: date
    ingested_at: datetime
    years: dict[int, YearRecord] = Field(default_factory=dict)


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset: str
    base_url: str
    last_extraction: ExtractionRecord | None = None
    history: list[ExtractionRecord] = Field(default_factory=list)


class IngestResult(BaseModel):
    """Saída de ``ingest_raw`` — payload serializável p/ XCom."""

    model_config = ConfigDict(extra="forbid")

    extraction_date: date
    changed: bool
    new_records: list[YearRecord] = Field(default_factory=list)
    unchanged_years: list[int] = Field(default_factory=list)
    failed_years: list[int] = Field(default_factory=list)


# =====================================================================
# Helpers de path
# =====================================================================
def _raw_root() -> Path:
    return get_settings().data_dir / "raw" / "hospitais-leitos" / "leitos"


def _staging_root() -> Path:
    return get_settings().data_dir / "staging" / "hospitais-leitos" / "leitos"


def _manifest_path() -> Path:
    return _raw_root() / MANIFEST_FILENAME


def _fallback_csv_for_year(year: int) -> Path:
    return get_settings().docs_dir / FALLBACK_RELATIVE / _csv_filename(year)


def _available_years() -> list[int]:
    """Anos candidatos: do FIRST_YEAR até o ano corrente, inclusive."""
    return list(range(FIRST_YEAR, date.today().year + 1))


# =====================================================================
# Persistência do manifesto (atomic write)
# =====================================================================
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
                f"ZIP {zip_path.name} não contém CSV: {zf.namelist()!r}"
            )
        with zf.open(members[0]) as src, target_csv.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=IO_CHUNK_BYTES)


# =====================================================================
# Processamento por ano
# =====================================================================
def _process_year_remote(
    year: int,
    previous: YearRecord | None,
    partition_dir: Path,
) -> tuple[YearRecord | None, bool]:
    """Tenta baixar o ano via S3 (caminho feliz).

    Returns:
        Tupla ``(record, is_new)``:
            * ``(None, False)``  → ano inalterado (ETag bate).
            * ``(record, True)`` → conteúdo novo, partição aterrissada.
            * ``(record, False)`` → ETag mudou mas hash do CSV é igual
              ao anterior; manifest deve refletir o novo ETag, mas
              não regravar parquet.
    """
    url = _source_url(year)
    etag, last_modified, content_length = _head_remote(url)
    logger.info(
        "HEAD %s → etag=%s last_modified=%s",
        url,
        etag,
        last_modified,
    )

    if _etag_matches(previous, etag, last_modified):
        assert previous is not None
        logger.info("  %d inalterado (ETag/Last-Modified bate)", year)
        return None, False

    target_csv = partition_dir / _csv_filename(year)
    zip_path = partition_dir / f"Leitos_csv_{year}.zip"
    try:
        _download_zip(url, zip_path)
        _extract_first_csv(zip_path, target_csv)
    finally:
        if zip_path.exists():
            zip_path.unlink()

    source_hash = _sha256_of(target_csv)

    if previous is not None and previous.source_hash == source_hash:
        logger.info(
            "  %d ETag mudou mas conteúdo é idêntico — atualizando manifest, "
            "removendo CSV duplicado",
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
    """Usa o CSV local em ``docs/``. Preserva imutabilidade do raw.

    Estratégia:
        1. Se o ``target_csv`` já existe na partição (re-run no mesmo
           dia), assume que é imutável e só calcula seu hash.
        2. Senão, calcula o hash da **fonte** antes de copiar. Se
           bate com ``previous.source_hash``, retorna ``unchanged``
           sem tocar disco — nenhuma cópia é feita.
        3. Só copia quando há conteúdo novo a aterrissar.

    Returns (idem do _process_year_remote).
    """
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
    """Aterrissa todos os anos disponíveis em ``data/raw/.../<YYYY-MM-DD>/``.

    Args:
        years: Lista de anos a processar. Default: ``2007..ano_corrente``.

    Returns:
        ``IngestResult`` com listas separadas de anos novos, inalterados
        e falhados. ``changed`` é ``True`` se algum ano foi atualizado.
    """
    if years is None:
        years = _available_years()

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

        # 1) Caminho feliz: S3 público.
        try:
            record, is_new = _process_year_remote(year, previous, partition_dir)
        except (requests.RequestException, RuntimeError, zipfile.BadZipFile) as exc:
            # 2) Cai para o fallback local SEM marcar como erro:
            #    o CKAN do MS hoje só publica os anos mais recentes;
            #    anos antigos voltam 403 e precisam vir do `docs/`.
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

    # ---- Persiste manifest se houve qualquer movimento ----
    if new_records or etag_only_updates:
        merged_years: dict[int, YearRecord] = dict(previous_years)
        for rec in new_records + etag_only_updates:
            merged_years[rec.year] = rec
        new_extraction = ExtractionRecord(
            extraction_date=today,
            ingested_at=datetime.now(tz=timezone.utc),
            years=merged_years,
        )
        if manifest.last_extraction is not None:
            manifest.history.append(manifest.last_extraction)
        manifest.last_extraction = new_extraction
        _save_manifest(manifest)

    # ---- Descarta a partição se NADA novo aterrissou ----
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
    """Converte cada CSV novo em Parquet particionado por data de extração.

    Apenas anos em ``result.new_records`` são processados — anos
    inalterados já têm Parquet de extrações anteriores e não precisam
    ser regravados.
    """
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

    # ---- Atualiza manifest com parquet_path de cada ano novo ----
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


__all__ = [
    "BASE_URL",
    "DATASET",
    "ExtractionRecord",
    "IngestResult",
    "Manifest",
    "YearRecord",
    "convert_to_parquet",
    "ingest_raw",
]
