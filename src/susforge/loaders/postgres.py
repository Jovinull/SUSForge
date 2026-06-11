"""Loader de alta performance Polars → PostgreSQL via COPY.

Usa o protocolo binário do Postgres (``COPY ... FROM STDIN``) através do
``psycopg2.cursor.copy_expert``, alimentado por um CSV em memória
gerado pelo próprio Polars (``DataFrame.write_csv`` para ``BytesIO``).
Esta é a forma mais rápida e econômica de mover centenas de milhares
de linhas para o Postgres sem materializar arquivos em disco.

Idempotência:
    * ``replace_partition``: transação ATÔMICA — opcionalmente
      ``DELETE WHERE <partition_column> = <value>`` (ou ``TRUNCATE``
      se sem coluna), seguido do ``COPY``. Falha em qualquer etapa
      aborta tudo via ``ROLLBACK``.

Não tentamos abstrair upsert ou MERGE ainda — quando o primeiro fato
incremental real chegar, evoluímos.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Any

import polars as pl
import psycopg2
from psycopg2.extensions import connection as PgConnection

from susforge.config import get_settings

logger = logging.getLogger(__name__)


def get_connection() -> PgConnection:
    """Abre uma conexão psycopg2 lendo a DSN de ``DatabaseSettings``."""
    return psycopg2.connect(get_settings().database.dsn)


def execute_ddl(ddl_path: Path, *, conn: PgConnection | None = None) -> int:
    """Aplica um arquivo SQL multi-statement (DDL+DML idempotentes).

    Útil tanto para schemas (``CREATE TABLE IF NOT EXISTS``) quanto
    para ELT in-database (``TRUNCATE`` + ``INSERT ... SELECT``).

    Returns:
        ``cur.rowcount`` da ÚLTIMA instrução executada. Para um SQL
        do tipo ``CREATE; TRUNCATE; INSERT``, isso é o número de
        linhas inseridas. Para um SQL só de DDL, será ``-1`` (psycopg2
        devolve isso quando a instrução não toca em linhas).
    """
    if not ddl_path.exists():
        raise FileNotFoundError(ddl_path)
    sql = ddl_path.read_text(encoding="utf-8")

    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    assert conn is not None
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rowcount = int(cur.rowcount)
        conn.commit()
        logger.info("✓ %s aplicado (rowcount=%d)", ddl_path.name, rowcount)
        return rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def count_rows(
    schema: str,
    table: str,
    *,
    where: str | None = None,
    conn: PgConnection | None = None,
) -> int:
    """Helper ``SELECT count(*)`` para relatórios pós-carga."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    assert conn is not None
    try:
        with conn.cursor() as cur:
            sql = f'SELECT count(*) FROM "{schema}"."{table}"'
            if where:
                sql += f" WHERE {where}"
            cur.execute(sql)
            return int(cur.fetchone()[0])
    finally:
        if own_conn:
            conn.close()


def _coerce_for_copy(df: pl.DataFrame) -> pl.DataFrame:
    """Pré-processa colunas para o formato CSV aceito pelo Postgres COPY.

    * Booleanos: ``True``/``False`` → ``"t"``/``"f"`` (forma curta aceita
      por TIMESTAMPTZ e BOOLEAN do Postgres em CSV).
    * Demais tipos: mantidos. Polars já serializa datetimes em ISO 8601
      com timezone e nulls como campo vazio.
    """
    bool_cols = [name for name, dtype in df.schema.items() if dtype == pl.Boolean]
    if not bool_cols:
        return df
    return df.with_columns(
        [
            pl.when(pl.col(c).is_null())
            .then(None)
            .when(pl.col(c))
            .then(pl.lit("t"))
            .otherwise(pl.lit("f"))
            .alias(c)
            for c in bool_cols
        ]
    )


def replace_partition(
    df: pl.DataFrame,
    *,
    schema: str,
    table: str,
    partition_column: str | None = None,
    partition_value: Any = None,
    columns: list[str] | None = None,
    conn: PgConnection | None = None,
) -> int:
    """Substitui uma partição lógica da tabela em uma única transação.

    Args:
        df: DataFrame Polars já validado, com colunas no mesmo nome/
            ordem da tabela alvo.
        schema: Schema do Postgres (ex.: ``"silver"``).
        table: Nome da tabela.
        partition_column: Coluna que define a partição lógica
            (ex.: ``"_extraction_date"``). Se ``None``, faz ``TRUNCATE``
            substituindo o conteúdo inteiro.
        partition_value: Valor da partição a ser substituída
            (ex.: ``date(2026, 6, 10)``).
        columns: Colunas a copiar — default = todas do DataFrame.
        conn: Conexão psycopg2 reutilizada; se None, abre uma própria.

    Returns:
        Número de linhas efetivamente carregadas.
    """
    if columns is None:
        columns = list(df.columns)

    df_copy = _coerce_for_copy(df.select(columns))

    full_table = f'"{schema}"."{table}"'
    quoted_cols = ", ".join(f'"{c}"' for c in columns)
    copy_sql = (
        f"COPY {full_table} ({quoted_cols}) FROM STDIN "
        f"WITH (FORMAT CSV, NULL '', HEADER FALSE)"
    )

    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    assert conn is not None

    try:
        with conn.cursor() as cur:
            if partition_column is None:
                logger.info("TRUNCATE %s", full_table)
                cur.execute(f"TRUNCATE TABLE {full_table}")
            else:
                logger.info(
                    "DELETE FROM %s WHERE %s = %r",
                    full_table,
                    partition_column,
                    partition_value,
                )
                cur.execute(
                    f'DELETE FROM {full_table} WHERE "{partition_column}" = %s',
                    (partition_value,),
                )
                deleted = cur.rowcount
                logger.info("  → %d linhas removidas da partição", deleted)

            buf = io.BytesIO()
            df_copy.write_csv(
                buf,
                include_header=False,
                datetime_format="%Y-%m-%dT%H:%M:%S%.f%z",
                date_format="%Y-%m-%d",
                quote_style="necessary",
            )
            buf.seek(0)

            logger.info(
                "COPY %s — %d linhas, %d colunas",
                full_table,
                df_copy.height,
                df_copy.width,
            )
            cur.copy_expert(copy_sql, buf)
            inserted = cur.rowcount

        conn.commit()
        logger.info("✓ %d linhas inseridas em %s", inserted, full_table)
        return int(inserted)
    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


__all__ = [
    "count_rows",
    "execute_ddl",
    "get_connection",
    "replace_partition",
]
