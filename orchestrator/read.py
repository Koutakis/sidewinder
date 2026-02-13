import polars as pl
from config.connection import get_mssql_connection


def read(source_dns: str, query: str) -> pl.DataFrame:
    conn = get_mssql_connection(
        source_dns
    )  # Currently only support sql server in the read but this is very easy to change in the future
    df = pl.read_database(query, conn)
    conn.close()
    return df
