"""Bronze • Vacinação PNI (escopo COVID na Silver) — extração paginada DEMAS.

Quarto molde de extrator do SUSForge: **API REST paginada**.

Escopo desta DAG (inicial):
    * Endpoint: ``/vacinacao/doses-aplicadas-pni-2021``
    * Filtro: ``uf_estabelecimento=SE`` (Sergipe)
    * Período: 2021 inteiro

Nota: a API DEMAS não oferece filtro por imunobiológico — esta DAG
puxa **todas as vacinas do PNI 2021/SE**. A separação COVID-19 fica
para a Silver (filtro por ``sg_vacina``).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from susforge.io.vacinacao_covid import (
    DATASET,
    IngestResult,
    convert_to_parquet,
    ingest_raw,
)

logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    "owner": "susforge",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="bronze_vacinacao_covid",
    description="Bronze · Vacinação PNI 2021/SE via API DEMAS (paginada REST)",
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "vacinacao", "covid", "demas-api", "rest-paginada"],
    doc_md=__doc__,
)
def bronze_vacinacao_covid() -> None:
    @task(task_id="ingest_raw")
    def ingest() -> dict[str, Any]:
        result = ingest_raw()
        if not result.changed:
            raise AirflowSkipException(
                f"[{DATASET}] payload inalterado desde {result.extraction_date} "
                f"(hash {result.payload_hash[:12]}…)"
            )
        print(
            f"[{DATASET}] nova ingestão: "
            f"itens={result.item_count} pages={result.page_count} "
            f"throughput={result.stats.get('items_per_second')}/s"
        )
        return {
            "extraction_date": result.extraction_date.isoformat(),
            "raw_dir": str(result.raw_dir),
            "page_count": result.page_count,
            "item_count": result.item_count,
            "payload_hash": result.payload_hash,
            "source": result.source,
            "stats": result.stats,
        }

    @task(task_id="to_parquet")
    def to_parquet(result_dict: dict[str, Any]) -> str:
        from datetime import date as _date

        result = IngestResult(
            extraction_date=_date.fromisoformat(result_dict["extraction_date"]),
            raw_dir=Path(result_dict["raw_dir"]),
            page_count=int(result_dict["page_count"]),
            item_count=int(result_dict["item_count"]),
            changed=True,
            payload_hash=result_dict["payload_hash"],
            source=result_dict["source"],
            stats=result_dict["stats"],
        )
        path = convert_to_parquet(result)
        print(f"[{DATASET}] parquet gerado em {path}")
        return str(path)

    to_parquet(ingest())


bronze_vacinacao_covid()
