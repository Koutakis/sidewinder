import polars as pl
from orchestrator.connection import get_mssql_connection


def read_query(source_dns: str, query: str) -> tuple[list[tuple], list[str]]:
    conn = get_mssql_connection(source_dns)
    cursor = conn.cursor()
    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows, columns


def read_polars(source_dns: str, query: str) -> pl.DataFrame:
    conn = get_mssql_connection(source_dns)
    df = pl.read_database(query, conn)
    conn.close()
    return df
