from __future__ import annotations
from typing import TYPE_CHECKING

import polars as pl
from config.connections import get_postgres_connection
from config.type_mapping import pg_type_from_polars

if TYPE_CHECKING:
    from core.model import ResolvedConfig


def _clean_row(row: tuple) -> tuple:
    return tuple(
        None
        if (isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")))
        else v
        for v in row
    )


def _build_ddl_from_df(df: pl.DataFrame) -> str:
    return ", ".join(
        f'"{col}" {pg_type_from_polars(df[col].dtype)}' for col in df.columns
    )


def write(cfg: ResolvedConfig, df: pl.DataFrame) -> None:
    conn = get_postgres_connection(cfg.dest_env)
    schema = cfg.destination_schema
    table = cfg.destination_table
    columns = df.columns
    col_defs = cfg.destination_ddl if cfg.destination_ddl else _build_ddl_from_df(df)

    with conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({col_defs})")
        conn.commit()

        if cfg.table_mode.value == "truncate_insert":
            conn.execute(f"TRUNCATE TABLE {schema}.{table}")
            conn.commit()

        if cfg.table_mode.value == "merge" and cfg.since and cfg.until:
            conn.execute(
                f'DELETE FROM {schema}.{table} WHERE "_data_modified" >= %s AND "_data_modified" < %s',
                (cfg.since, cfg.until),
            )
            conn.commit()

        col_names = ", ".join(f'"{col}"' for col in columns)
        rows = df.rows()

        with conn.cursor() as cursor:
            with cursor.copy(f"COPY {schema}.{table} ({col_names}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(_clean_row(row))

        conn.commit()

        if cfg.destination_indexes:
            for col in cfg.destination_indexes:
                index_name = f"idx_{table}_{col}"
                conn.execute(
                    f'CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{table} ("{col}")'
                )
            conn.commit()
