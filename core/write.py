from __future__ import annotations
from typing import TYPE_CHECKING

import polars as pl
from config.connections import get_postgres_connection
from config.type_mapping import pg_type_from_polars

if TYPE_CHECKING:
    from bollhav import Model


def _clean_row(row: tuple) -> tuple:
    return tuple(
        None
        if (isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")))
        else v
        for v in row
    )


def _build_ddl_from_config(columns: dict) -> str:
    return ", ".join(
        f'"{name}" {pg_type_from_polars(dtype)}' for name, dtype in columns.items()
    )


def _build_ddl_from_df(df: pl.DataFrame) -> str:
    return ", ".join(
        f'"{col}" {pg_type_from_polars(df[col].dtype)}' for col in df.columns
    )


def write(cfg: Model, df: pl.DataFrame, dest_env: str, since: str | None = None, until: str | None = None) -> None:
    conn = get_postgres_connection(dest_env)
    schema = cfg.destination_schema
    table = cfg.destination_table
    columns = df.columns
    col_defs = _build_ddl_from_config(cfg.columns) if cfg.columns else _build_ddl_from_df(df)

    with conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({col_defs})")
        conn.commit()

        if cfg.write_mode.value == "TRUNCATE_INSERT":
            conn.execute(f"TRUNCATE TABLE {schema}.{table}")
            conn.commit()

        if cfg.write_mode.value == "MERGE" and since and until:
            conn.execute(
                f'DELETE FROM {schema}.{table} WHERE "_data_modified" >= %s AND "_data_modified" < %s',
                (since, until),
            )
            conn.commit()

        col_names = ", ".join(f'"{col}"' for col in columns)
        rows = df.rows()

        with conn.cursor() as cursor:
            with cursor.copy(f"COPY {schema}.{table} ({col_names}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(_clean_row(row))

        conn.commit()


def write_view(cfg: Model, dest_env: str, view_query: str) -> None:
    conn = get_postgres_connection(dest_env)
    schema = cfg.destination_schema
    table = cfg.destination_table

    with conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"CREATE OR REPLACE VIEW {schema}.{table} AS {view_query}")
        conn.commit()
