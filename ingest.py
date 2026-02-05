import time
import pyodbc
import psycopg
from datetime import datetime
from roskarl import env_var_dsn
from enum import Enum
from typing import Callable, Optional
import polars as pl

class TableMode(Enum):
    FULL = "replace"
    INCREMENTAL = "append"


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


def read_query(source_dns: str, query: str) -> tuple[list[tuple], list[str]]:
    """Read data from MSSQL using a SQL query"""
    conn_string = _build_mssql_conn(source_dns)
    conn = pyodbc.connect(conn_string, timeout=600)
    conn.timeout = 600
    cursor = conn.cursor()
    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows, columns


def read_polars(source_dns: str, query: str) -> pl.DataFrame:
    """Read data from MSSQL into Polars DataFrame"""
    conn_string = _build_mssql_conn(source_dns)
    conn = pyodbc.connect(conn_string, timeout=600)
    
    df = pl.read_database(query, conn)
    conn.close()
    
    return df


def write(
    dest_env: str,
    dest_table: str,
    rows: list[tuple],
    columns: list[str],
    table_mode: str = "fail",
    col_types: dict[str, str] | None = None
) -> None:
    conn_string = _build_postgres_conn(dest_env)
    schema, table = _parse_table(dest_table)

    with psycopg.connect(conn_string) as pg_conn:
        if table_mode == 'replace':
            pg_conn.execute(f'DROP TABLE IF EXISTS {schema}.{table}')
            pg_conn.commit()

        if table_mode in ['replace', 'fail']:
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
    """Write Polars DataFrame to Postgres"""
    conn_string = _build_postgres_conn(dest_env)
    schema, table = _parse_table(dest_table)
    
    with psycopg.connect(conn_string) as conn:
        if table_mode == 'replace':
            conn.execute(f'DROP TABLE IF EXISTS {schema}.{table}')
            conn.commit()
        
        # Convert to list of tuples for writing
        rows = df.to_dicts()
        columns = df.columns
        
        if rows:
            # Infer types from first row
            col_defs = []
            for col in columns:
                val = rows[0][col]
                pg_type = _infer_pg_type(val)
                col_defs.append(f'"{col}" {pg_type}')
            
            if table_mode in ['replace', 'fail']:
                conn.execute(f'CREATE TABLE IF NOT EXISTS {schema}.{table} ({", ".join(col_defs)})')
                conn.commit()
            
            # Use COPY for fast insert
            col_names = ', '.join(f'"{col}"' for col in columns)
            with conn.cursor() as cursor:
                with cursor.copy(f'COPY {schema}.{table} ({col_names}) FROM STDIN') as copy:
                    for row_dict in rows:
                        row_tuple = tuple(row_dict[col] for col in columns)
                        copy.write_row(_clean_row(row_tuple))
            
            conn.commit()


def run_ingest(
    dest_env: str,
    dest_table: str,
    execute: Callable,
    table_mode: TableMode = TableMode.FULL,
    start: Optional[str] = None,
    end: Optional[str] = None,
    verbose: bool = True,
    **kwargs
) -> dict:
    """
    Run ingest using a Python callable (function) that returns data.
    
    Args:
        dest_env: Destination database env var name
        dest_table: Destination schema.table
        execute: Function that returns (rows, columns) or polars.DataFrame
        table_mode: FULL or INCREMENTAL
        start: Optional start date for incremental loads
        end: Optional end date for incremental loads
        **kwargs: Additional arguments passed to execute function
    """
    start_total = time.time()

    if verbose:
        date_range = f" ({start} to {end})" if start and end else ""
        print(f"[{datetime.now()}] Executing {dest_table}{date_range}...")

    start_exec = time.time()
    
    # Call the execute function with date range and kwargs
    result = execute(start=start, end=end, **kwargs)
    
    # Handle different return types
    if isinstance(result, pl.DataFrame):
        df = result
        rows_count = len(df)
        exec_time = time.time() - start_exec
        
        if verbose:
            print(f"  ✓ {rows_count:,} rows in {exec_time:.2f}s")
            print(f"[{datetime.now()}] Writing to {dest_table}...")
        
        start_write = time.time()
        write_polars(dest_env, dest_table, df, table_mode.value)
        write_time = time.time() - start_write
        
    else:
        # Assume tuple of (rows, columns)
        rows, columns = result
        rows_count = len(rows)
        exec_time = time.time() - start_exec
        
        if verbose:
            print(f"  ✓ {rows_count:,} rows in {exec_time:.2f}s")
            print(f"[{datetime.now()}] Writing to {dest_table}...")
        
        start_write = time.time()
        write(dest_env, dest_table, rows, columns, table_mode.value)
        write_time = time.time() - start_write
    
    total_time = time.time() - start_total

    if verbose:
        print(f"  ✓ Written in {write_time:.2f}s")
        print(f"  Total: {total_time:.2f}s")

    return {
        'rows': rows_count,
        'exec_time': exec_time,
        'write_time': write_time,
        'total_time': total_time,
    }
