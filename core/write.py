from __future__ import annotations
from typing import TYPE_CHECKING

import polars as pl
from config.connections import get_postgres_connection
from config.type_mapping import pg_type_from_polars

if TYPE_CHECKING:
    from bollhav import Model
    from roskarl import DSN


def _clean_row(row: tuple) -> tuple:
    return tuple(
        None
        if (isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")))
        else v
        for v in row
    )


def _build_ddl_from_config(columns: list) -> str:
    parts = []
    for col in columns:
        definition = f'"{col.name}" {col.data_type.value}'
        if col.precision is not None:
            if col.scale is not None:
                definition += f"({col.precision},{col.scale})"
            else:
                definition += f"({col.precision})"
        if col.length is not None:
            definition += f"({col.length})"
        if not col.nullable:
            definition += " NOT NULL"
        if col.primary_key:
            definition += " PRIMARY KEY"
        if col.unique and not col.primary_key:
            definition += " UNIQUE"
        parts.append(definition)
    return ", ".join(parts)


def _build_ddl_from_df(df: pl.DataFrame) -> str:
    return ", ".join(
        f'"{col}" {pg_type_from_polars(df[col].dtype)}' for col in df.columns
    )


def write(cfg: Model, df: pl.DataFrame, dest_dsn: DSN, since: str | None = None, until: str | None = None) -> None:
    conn = get_postgres_connection(dest_dsn)
    schema = cfg.schema
    table = cfg.table
    columns = df.columns
    col_defs = _build_ddl_from_config(cfg.columns) if cfg.columns else _build_ddl_from_df(df)

    with conn:
        # DDL is separate — safe to commit alone
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({col_defs})")
        conn.commit()

        # Delete + insert in one transaction — all or nothing
        if cfg.write_mode.value == "TRUNCATE_INSERT":
            conn.execute(f"TRUNCATE TABLE {schema}.{table}")

        if cfg.write_mode.value == "MERGE" and since and until:
            conn.execute(
                f'DELETE FROM {schema}.{table} WHERE "_data_modified" >= %s AND "_data_modified" < %s',
                (since, until),
            )

        col_names = ", ".join(f'"{col}"' for col in columns)
        rows = df.rows()

        with conn.cursor() as cursor:
            with cursor.copy(f"COPY {schema}.{table} ({col_names}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(_clean_row(row))

        conn.commit()


def write_view(cfg: Model, dest_dsn: DSN, view_query: str) -> None:
    conn = get_postgres_connection(dest_dsn)
    schema = cfg.schema
    table = cfg.table

    with conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"CREATE OR REPLACE VIEW {schema}.{table} AS {view_query}")
        conn.commit()
