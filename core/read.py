from collections.abc import Generator
import polars as pl
from config.connections import get_mssql_connection
from roskarl import env_var_dsn, DSN


def read(source_env: str, query: str, batch_size: int = 100_000) -> Generator[pl.DataFrame, None, None]:
    dsn: DSN = env_var_dsn(name=source_env)
    conn = get_mssql_connection(source_env)
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield pl.DataFrame([dict(zip(columns, row)) for row in rows])

    cursor.close()
    conn.close()
