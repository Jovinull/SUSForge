"""Extrator Bronze de Vacinação (PNI/COVID) via API DEMAS — paginação REST.

Quarto molde de extrator do SUSForge: **API REST paginada com cap**.

Escopo desta iteração inicial:
    * Endpoint: ``/vacinacao/doses-aplicadas-pni-2021``
    * Filtro: ``uf_estabelecimento=SE`` (Sergipe)
    * Período: ano 2021 (inteiro no endpoint anual)
    * Volume esperado: ~5k doses (a API tem cap prático ~100 páginas)

Nota importante sobre escopo COVID
----------------------------------
A API DEMAS NÃO oferece filtro por imunobiológico — o endpoint PNI
devolve TODAS as vacinas aplicadas (BCG, dTpa, COVID, HepB…). O Bronze
segue a regra de ouro e baixa tudo. A separação COVID-19 fica para a
Silver, filtrando ``sg_vacina`` ou ``descricao_vacina``.

Padrão Ouro Bronze (adaptado a API REST):
    * Particionamento por data de extração.
    * Manifesto leve com ``last_extraction`` e ``history[]``.
    * Detecção de mudança: hash do conjunto de páginas baixadas
      (comparação de hash do payload final). ETag/Last-Modified não
      se aplicam a respostas REST dinâmicas.
    * Imutabilidade: páginas JSON brutas preservadas, Parquet derivado.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Final, Literal

import polars as pl
import requests
from pydantic import BaseModel, ConfigDict, Field

from susforge.config import get_settings
from susforge.io.demas_api import MAX_LIMIT, DemasClient

logger = logging.getLogger(__name__)

DATASET: Final = "vacinacao_covid"
DEMAS_PATH: Final = "/vacinacao/doses-aplicadas-pni-2021"
DEMAS_DATA_KEY: Final = "doses_aplicadas_pni"
SCOPE_YEAR: Final = 2021
SCOPE_UF: Final = "SE"
PAGE_LIMIT: Final = MAX_LIMIT
MAX_PAGES: Final = 200
PARQUET_COMPRESSION: Final = "zstd"
MANIFEST_FILENAME: Final = "manifest.json"

Source = Literal["remote", "unchanged"]


# =====================================================================
# Modelos
# =====================================================================
class ExtractionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    extraction_date: date
    ingested_at: datetime
    scope_year: int
    scope_uf: str
    pages_fetched: int
    last_nonempty_offset: int
    total_items: int
    total_seconds: float
    items_per_second: float
    bytes_downloaded: int
    payload_hash: str
    raw_dir: str
    parquet_path: str | None = None


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset: str
    endpoint: str
    last_extraction: ExtractionRecord | None = None
    history: list[ExtractionRecord] = Field(default_factory=list)


class IngestResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    extraction_date: date
    raw_dir: Path
    page_count: int
    item_count: int
    changed: bool
    payload_hash: str
    source: Source
    stats: dict[str, Any] = Field(default_factory=dict)


# =====================================================================
# Paths
# =====================================================================
def _raw_root() -> Path:
    return get_settings().data_dir / "raw" / "vacinacao" / "covid"


def _staging_root() -> Path:
    return get_settings().data_dir / "staging" / "vacinacao" / "covid"


def _manifest_path() -> Path:
    return _raw_root() / MANIFEST_FILENAME


def _load_manifest() -> Manifest:
    path = _manifest_path()
    if not path.exists():
        return Manifest(dataset=DATASET, endpoint=DEMAS_PATH)
    return Manifest.model_validate_json(path.read_text(encoding="utf-8"))


def _save_manifest(manifest: Manifest) -> None:
    path = _manifest_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    tmp.replace(path)


def _payload_hash(items: list[dict[str, Any]]) -> str:
    """Hash determinístico do payload — usa o conjunto de codigo_documento.

    Doses são imutáveis (UUID estável), então o conjunto de UUIDs
    identifica o lote univocamente. Independe de ordem.
    """
    h = hashlib.sha256()
    for code in sorted(
        i.get("codigo_documento", "") for i in items if i.get("codigo_documento")
    ):
        h.update(code.encode("utf-8"))
    return h.hexdigest()


def latest_extraction_date() -> date:
    manifest = _load_manifest()
    if manifest.last_extraction is None:
        raise RuntimeError(
            "Nenhuma extração Vacinação disponível — rode 'bronze_vacinacao_covid'"
        )
    return manifest.last_extraction.extraction_date


# =====================================================================
# Núcleo da extração
# =====================================================================
def _persist_pages_as_json(
    items: list[dict[str, Any]],
    partition_dir: Path,
    *,
    page_size: int = PAGE_LIMIT,
) -> int:
    """Salva páginas como arquivos NDJSON no raw (imutável)."""
    partition_dir.mkdir(parents=True, exist_ok=True)
    page_count = 0
    for i in range(0, len(items), page_size):
        page = items[i : i + page_size]
        page_idx = i // page_size
        target = partition_dir / f"page_{page_idx:04d}.json"
        target.write_text(
            json.dumps(page, ensure_ascii=False, indent=None),
            encoding="utf-8",
        )
        page_count += 1
    return page_count


def ingest_raw() -> IngestResult:
    """Pagina a API DEMAS e aterrissa páginas JSON em ``data/raw/``.

    Returns:
        ``IngestResult`` com estatísticas de throughput e estado de
        mudança vs última extração.
    """
    manifest = _load_manifest()
    today = date.today()
    partition_dir = (
        _raw_root()
        / today.isoformat()
        / f"ano={SCOPE_YEAR}"
        / f"uf={SCOPE_UF}"
    )

    logger.info(
        "Iniciando paginação DEMAS — ano=%d uf=%s limit=%d max_pages=%d",
        SCOPE_YEAR,
        SCOPE_UF,
        PAGE_LIMIT,
        MAX_PAGES,
    )

    client = DemasClient()
    try:
        items, stats = client.paginate(
            DEMAS_PATH,
            params={"uf_estabelecimento": SCOPE_UF},
            data_key=DEMAS_DATA_KEY,
            limit=PAGE_LIMIT,
            max_pages=MAX_PAGES,
        )
    except requests.RequestException:
        logger.exception("Falha na paginação DEMAS — ver tracebacks")
        raise

    logger.info(
        "Coletado: %d itens em %.2fs (%.1f items/s, %d KiB)",
        stats["total_items"],
        stats["total_seconds"],
        stats["items_per_second"],
        stats["bytes_downloaded"] // 1024,
    )

    payload_hash = _payload_hash(items)
    previous = manifest.last_extraction
    if previous is not None and previous.payload_hash == payload_hash:
        logger.info(
            "Payload inalterado (hash bate com %s) — pulando aterrissagem",
            previous.extraction_date,
        )
        return IngestResult(
            extraction_date=previous.extraction_date,
            raw_dir=Path(previous.raw_dir),
            page_count=previous.pages_fetched,
            item_count=previous.total_items,
            changed=False,
            payload_hash=payload_hash,
            source="unchanged",
            stats=stats,
        )

    # Persiste páginas como arquivos NDJSON
    page_count = _persist_pages_as_json(items, partition_dir)
    logger.info(
        "Gravadas %d páginas em %s", page_count, partition_dir
    )

    record = ExtractionRecord(
        extraction_date=today,
        ingested_at=datetime.now(tz=timezone.utc),
        scope_year=SCOPE_YEAR,
        scope_uf=SCOPE_UF,
        pages_fetched=int(stats["pages_fetched"]),
        last_nonempty_offset=int(stats["last_nonempty_offset"]),
        total_items=int(stats["total_items"]),
        total_seconds=float(stats["total_seconds"]),
        items_per_second=float(stats["items_per_second"]),
        bytes_downloaded=int(stats["bytes_downloaded"]),
        payload_hash=payload_hash,
        raw_dir=str(partition_dir),
    )
    if manifest.last_extraction is not None:
        manifest.history.append(manifest.last_extraction)
    manifest.last_extraction = record
    _save_manifest(manifest)

    return IngestResult(
        extraction_date=today,
        raw_dir=partition_dir,
        page_count=page_count,
        item_count=len(items),
        changed=True,
        payload_hash=payload_hash,
        source="remote",
        stats=stats,
    )


def convert_to_parquet(result: IngestResult) -> Path:
    """Concatena todas as páginas JSON do raw em um Parquet zstd."""
    partition_dir = (
        _staging_root() / result.extraction_date.isoformat()
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    target_parquet = partition_dir / f"pni_{SCOPE_YEAR}_{SCOPE_UF}.parquet"

    pages = sorted(result.raw_dir.glob("page_*.json"))
    if not pages:
        raise FileNotFoundError(
            f"Sem páginas JSON em {result.raw_dir} para converter"
        )

    all_items: list[dict[str, Any]] = []
    for p in pages:
        all_items.extend(json.loads(p.read_text(encoding="utf-8")))

    df = pl.from_dicts(all_items, infer_schema_length=None)
    # Cast tudo para String (regra Bronze — Silver tipa)
    df = df.with_columns([pl.col(c).cast(pl.String) for c in df.columns])

    df = df.with_columns(
        pl.lit(datetime.now(tz=timezone.utc)).alias("_ingested_at"),
        pl.lit(f"pni_{SCOPE_YEAR}_{SCOPE_UF}").alias("_source_file"),
        pl.lit(result.payload_hash).alias("_source_hash"),
    )

    logger.info(
        "Gravando Parquet %s (%d linhas × %d cols)",
        target_parquet,
        df.height,
        df.width,
    )
    df.write_parquet(target_parquet, compression=PARQUET_COMPRESSION)

    # Atualiza manifest com parquet_path
    manifest = _load_manifest()
    if (
        manifest.last_extraction is not None
        and manifest.last_extraction.extraction_date == result.extraction_date
    ):
        manifest.last_extraction.parquet_path = str(target_parquet)
        _save_manifest(manifest)

    return target_parquet


__all__ = [
    "DATASET",
    "DEMAS_PATH",
    "SCOPE_UF",
    "SCOPE_YEAR",
    "ExtractionRecord",
    "IngestResult",
    "Manifest",
    "convert_to_parquet",
    "ingest_raw",
    "latest_extraction_date",
]
