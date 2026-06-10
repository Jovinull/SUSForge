"""Bronze • Registro de Ocupação Hospitalar COVID-19 (2020–2022).

Vide ``susforge.io.covid_ocupacao``. Padrão Ouro Bronze (molde
Hospitais e Leitos — multi-ano), adaptado para CSV direto (sem ZIP).

Dataset descontinuado: 3 anos fechados, sem novos ingressos previstos.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from susforge.io.covid_ocupacao import (
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
    dag_id="bronze_covid_ocupacao",
    description="Bronze · Ocupação Hospitalar COVID-19 — 2020–2022, anual",
    start_date=datetime(2026, 6, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "covid", "assistencia-saude", "serie-historica"],
    doc_md=__doc__,
)
def bronze_covid_ocupacao() -> None:
    @task(task_id="ingest_raw")
    def ingest() -> dict[str, Any]:
        result = ingest_raw()
        if not result.changed:
            raise AirflowSkipException(
                f"[{DATASET}] nenhum ano novo — "
                f"inalterados={len(result.unchanged_years)} "
                f"falhados={len(result.failed_years)}"
            )
        anos = ", ".join(str(r.year) for r in result.new_records)
        print(
            f"[{DATASET}] novos {len(result.new_records)} anos: {anos} | "
            f"inalterados={len(result.unchanged_years)} "
            f"falhados={len(result.failed_years)}"
        )
        return result.model_dump(mode="json")

    @task(task_id="to_parquet")
    def to_parquet(result_dict: dict[str, Any]) -> list[str]:
        result = IngestResult.model_validate(result_dict)
        paths = convert_to_parquet(result)
        print(f"[{DATASET}] {len(paths)} parquet(s) gerado(s)")
        return [str(p) for p in paths]

    to_parquet(ingest())


bronze_covid_ocupacao()
