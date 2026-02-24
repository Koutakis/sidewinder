import pyodbc
import psycopg
from roskarl import env_var_dsn, DSN


def get_postgres_connection(dsn: DSN) -> psycopg.Connection:
    conn_string = f"host={dsn.hostname} port={dsn.port} dbname={dsn.database} user={dsn.username} password={dsn.password}"
    return psycopg.connect(conn_string)


def get_mssql_connection(dsn: DSN, timeout: int = 600) -> pyodbc.Connection:
    conn_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={dsn.hostname},{dsn.port};"
        f"DATABASE={dsn.database};"
        f"UID={dsn.username};"
        f"PWD={dsn.password};"
        f"TrustServerCertificate=yes"
    )
    conn = pyodbc.connect(conn_string, timeout=timeout)
    conn.timeout = timeout
    return conn



