import psycopg
import polars as pl
from datetime import datetime
from orchestrator.connection import get_postgres_connection


def _parse_table(dest_table: str) -> tuple[str, str]:
    if '.' in dest_table:
        schema, table = dest_table.split('.', 1)
    else:
        schema = 'public'
        table = dest_table
    return schema, table


def _infer_pg_type(value: object) -> str:
    if value is None:
        return 'TEXT'
    type_map: dict[type, str] = {
        int: 'BIGINT',
        float: 'DOUBLE PRECISION',
        bool: 'BOOLEAN',
        datetime: 'TIMESTAMPTZ',
        str: 'TEXT',
    }
    return type_map.get(type(value), 'TEXT')


def _clean_row(row: tuple) -> tuple:
    return tuple(
        None if (isinstance(v, float) and (v != v or v == float('inf') or v == float('-inf')))
        else v
        for v in row
    )


def write(
    dest_env: str,
    dest_table: str,
    rows: list[tuple],
    columns: list[str],
    table_mode: str = "fail",
    col_types: dict[str, str] | None = None
) -> None:
    schema, table = _parse_table(dest_table)

    with get_postgres_connection(dest_env) as pg_conn:
        if table_mode == 'replace':
            pg_conn.execute(f'DROP TABLE IF EXISTS {schema}.{table}')
            pg_conn.commit()

        if table_mode in ['replace', 'fail', 'append']:  # Add 'append' here
            if rows:
                if col_types:
                    col_defs = [f'"{col}" {col_types[col]}' for col in columns]
                else:
                    col_defs = [f'"{col}" {_infer_pg_type(val)}' for col, val in zip(columns, rows[0])]
                pg_conn.execute(f'CREATE TABLE IF NOT EXISTS {schema}.{table} ({", ".join(col_defs)})')
                pg_conn.commit()

        col_names = ', '.join(f'"{col}"' for col in columns)

        with pg_conn.cursor() as cursor:
            with cursor.copy(f'COPY {schema}.{table} ({col_names}) FROM STDIN') as copy:
                for row in rows:
                    copy.write_row(_clean_row(row))

        pg_conn.commit()


def write_polars(
    dest_env: str,
    dest_table: str,
    df: pl.DataFrame,
    table_mode: str = "fail"
) -> None:
    schema, table = _parse_table(dest_table)

    with get_postgres_connection(dest_env) as conn:
        if table_mode == 'replace':
            conn.execute(f'DROP TABLE IF EXISTS {schema}.{table}')
            conn.commit()

        rows = df.to_dicts()
        columns = df.columns

        if rows:
            col_defs = []
            for col in columns:
                val = rows[0][col]
                pg_type = _infer_pg_type(val)
                col_defs.append(f'"{col}" {pg_type}')

            if table_mode in ['replace', 'fail', 'append']:  # Add 'append' here
                conn.execute(f'CREATE TABLE IF NOT EXISTS {schema}.{table} ({", ".join(col_defs)})')
                conn.commit()

            col_names = ', '.join(f'"{col}"' for col in columns)
            with conn.cursor() as cursor:
                with cursor.copy(f'COPY {schema}.{table} ({col_names}) FROM STDIN') as copy:
                    for row_dict in rows:
                        row_tuple = tuple(row_dict[col] for col in columns)
                        copy.write_row(_clean_row(row_tuple))

            conn.commit()
