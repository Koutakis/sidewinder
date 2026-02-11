from datetime import datetime
import polars as pl
import psycopg
from config.connection import get_postgres_connection
from config.type_mapping import pg_type_from_polars

def _parse_table(dest_table: str) -> tuple[str, str]:
    if "." in dest_table:
        schema, table = dest_table.split(".", 1)
    else:
        schema = "public"
        table = dest_table
    return schema, table


def _infer_pg_type(value: object) -> str:
    if value is None:
        return "TEXT"
    type_map: dict[type, str] = {
        int: "BIGINT",
        float: "DOUBLE PRECISION",
        bool: "BOOLEAN",
        datetime: "TIMESTAMPTZ",
        str: "TEXT",
    }
    return type_map.get(type(value), "TEXT")


def _clean_row(row: tuple) -> tuple:
    return tuple(
        None
        if (isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")))
        else v
        for v in row
    )


def write(
    dest_env: str,
    dest_table: str,
    rows: list[tuple],
    columns: list[str],
    table_mode: str = "fail",
    col_types: dict[str, str] | None = None,
) -> None:
    conn = get_postgres_connection(dest_env)
    schema, table = _parse_table(dest_table)

    with conn:
        if table_mode == "replace":
            conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
            conn.commit()

        if table_mode in ["replace", "fail"]:
            if rows:
                if col_types:
                    col_defs = [f'"{col}" {col_types[col]}' for col in columns]
                else:
                    col_defs = [
                        f'"{col}" {_infer_pg_type(val)}'
                        for col, val in zip(columns, rows[0])
                    ]
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({', '.join(col_defs)})"
                )
                conn.commit()

        col_names = ", ".join(f'"{col}"' for col in columns)

        with conn.cursor() as cursor:
            with cursor.copy(f"COPY {schema}.{table} ({col_names}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(_clean_row(row))

        conn.commit()


def write_polars(
    dest_env: str, dest_table: str, df: pl.DataFrame, table_mode: str = "fail"
) -> None:
    conn = get_postgres_connection(dest_env)
    schema, table = _parse_table(dest_table)
    columns = df.columns

    with conn:
        if table_mode == "replace":
            conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
            conn.commit()

        if table_mode in ["replace", "fail"]:
            col_defs = [f'"{col}" {pg_type_from_polars(df[col].dtype)}' for col in columns]
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({', '.join(col_defs)})"
            )
            conn.commit()

        col_names = ", ".join(f'"{col}"' for col in columns)
        rows = df.rows()

        with conn.cursor() as cursor:
            with cursor.copy(f"COPY {schema}.{table} ({col_names}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(_clean_row(row))

        conn.commit()
