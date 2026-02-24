from collections.abc import Generator
import polars as pl
from config.connections import get_mssql_connection
from roskarl import env_var_dsn


def read(env_name: str, query: str, batch_size: int | None = None) -> Generator[pl.DataFrame, None, None]:
    dsn = env_var_dsn(name=env_name)
    conn = get_mssql_connection(dsn)

    if batch_size is None:
        df = pl.read_database(query, conn)
        conn.close()
        yield df
    else:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield pl.DataFrame(
                {col: [row[i] for row in rows] for i, col in enumerate(columns)}
            )

        cursor.close()
        conn.close()
