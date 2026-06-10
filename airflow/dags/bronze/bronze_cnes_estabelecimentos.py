"""Bronze • CNES Estabelecimentos — ingestão nacional CSV → Parquet.

Padrão Ouro Bronze (vide ``susforge.io.cnes``):
    * Particionamento por data de extração em ``raw/`` e ``staging/``.
    * Detecção de mudança via HEAD (ETag / Last-Modified) ou hash.
    * Schedule ``@weekly`` — a task de ingestão é idempotente:
      checa o S3, e só baixa/converte se houver mudança real.
    * Sem ``force_download``: a idempotência mora no extrator.

Fora de escopo (Silver / Gold):
    * Decodificação de domínios, tipagem estrita, mascaramento de PII,
      carga em banco.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from susforge.io.cnes import (
    DATASET,
    IngestResult,
    convert_to_parquet,
    ingest_raw,
)

DEFAULT_ARGS = {
    "owner": "susforge",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="bronze_cnes_estabelecimentos",
    description=(
        "Bronze · CNES Estabelecimentos — CSV→Parquet, particionado por data"
    ),
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "cnes", "assistencia-saude", "master-data"],
    doc_md=__doc__,
)
def bronze_cnes_estabelecimentos() -> None:
    @task(task_id="ingest_raw")
    def ingest() -> dict[str, Any]:
        """Verifica remoto e baixa só se mudou. Skipa a DAG se inalterado."""
        result = ingest_raw()
        if not result.changed:
            raise AirflowSkipException(
                f"[{DATASET}] inalterado desde {result.extraction_date} "
                f"(source={result.source}) — conversão pulada"
            )
        print(
            f"[{DATASET}] nova ingestão: date={result.extraction_date} "
            f"source={result.source} hash={result.source_hash[:16]}…"
        )
        return result.model_dump(mode="json")

    @task(task_id="to_parquet")
    def to_parquet(result_dict: dict[str, Any]) -> str:
        """Converte CSV → Parquet particionado por data de extração."""
        result = IngestResult.model_validate(result_dict)
        path = convert_to_parquet(result)
        print(f"[{DATASET}] parquet gerado em {path}")
        return str(path)

    to_parquet(ingest())


bronze_cnes_estabelecimentos()
