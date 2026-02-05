import pyodbc
import psycopg
from roskarl import env_var_dsn


def _build_mssql_conn(source_dns: str) -> str:
    dsn = env_var_dsn(name=source_dns)
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={dsn.hostname},{dsn.port};"
        f"DATABASE={dsn.database};"
        f"UID={dsn.username};"
        f"PWD={dsn.password};"
        f"TrustServerCertificate=yes"
    )


def _build_postgres_conn(dest_env: str) -> str:
    dsn = env_var_dsn(name=dest_env)
    return f"host={dsn.hostname} port={dsn.port} dbname={dsn.database} user={dsn.username} password={dsn.password}"


def get_mssql_connection(source_dns: str, timeout: int = 600) -> pyodbc.Connection:
    conn_string = _build_mssql_conn(source_dns)
    conn = pyodbc.connect(conn_string, timeout=timeout)
    conn.timeout = timeout
    return conn


def get_postgres_connection(dest_env: str) -> psycopg.Connection:
    conn_string = _build_postgres_conn(dest_env)
    return psycopg.connect(conn_string)
