"""Bronze • UBS — ingestão nacional CSV → Parquet.

Vide ``susforge.io.ubs``. Padrão Ouro Bronze (molde CNES — arquivo único).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from susforge.io.ubs import (
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
    dag_id="bronze_ubs",
    description="Bronze · UBS — CSV→Parquet, particionado por data",
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "ubs", "assistencia-saude", "master-data"],
    doc_md=__doc__,
)
def bronze_ubs() -> None:
    @task(task_id="ingest_raw")
    def ingest() -> dict[str, Any]:
        result = ingest_raw()
        if not result.changed:
            raise AirflowSkipException(
                f"[{DATASET}] inalterado desde {result.extraction_date} "
                f"(source={result.source})"
            )
        print(
            f"[{DATASET}] nova ingestão: date={result.extraction_date} "
            f"source={result.source} hash={result.source_hash[:16]}…"
        )
        return result.model_dump(mode="json")

    @task(task_id="to_parquet")
    def to_parquet(result_dict: dict[str, Any]) -> str:
        result = IngestResult.model_validate(result_dict)
        path = convert_to_parquet(result)
        print(f"[{DATASET}] parquet gerado em {path}")
        return str(path)

    to_parquet(ingest())


bronze_ubs()
