import polars as pl
from config.connections import get_postgres_connection
from config.type_mapping import pg_type_from_polars


def _parse_table(dest_table: str, dest_schema: str) -> tuple[str, str]:
    return dest_schema, dest_table


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


def write(
    dest_env: str,
    df: pl.DataFrame,
    schema: str,
    table: str,
    table_mode: str,
    ddl: str | None = None,
    since: str | None = None,
    until: str | None = None,
) -> None:
    conn = get_postgres_connection(dest_env)
    columns = df.columns

    col_defs = ddl if ddl else _build_ddl_from_df(df)

    with conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({col_defs})"
        )
        conn.commit()

        if table_mode == "truncate_insert":
            conn.execute(f"TRUNCATE TABLE {schema}.{table}")
            conn.commit()

        if table_mode == "merge" and since and until:
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


def create_indexes(
    dest_env: str,
    schema: str,
    table: str,
    indexes: list[str],
) -> None:
    if not indexes:
        return

    conn = get_postgres_connection(dest_env)
    with conn:
        for col in indexes:
            index_name = f"idx_{table}_{col}"
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{table} ("{col}")'
            )
        conn.commit()
