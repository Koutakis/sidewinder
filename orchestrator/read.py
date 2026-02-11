import polars as pl
from config.connection import get_mssql_connection


def read(source_dns: str, query: str) -> pl.DataFrame:
    conn = get_mssql_connection(source_dns)
    df = pl.read_database(query, conn)
    conn.close()
    return df
