import time
import os
import polars as pl
from datetime import datetime
from typing import Callable
from core.model import ModelConfig, TableMode


def run_ingest(
    execute: Callable[..., pl.DataFrame],
    dest_env: str | None = None,
    since: str | None = None,
    until: str | None = None,
    table_mode: TableMode | None = None,
    verbose: bool = True,
    **kwargs,
) -> dict:
    from core.write import write, create_indexes

    config: ModelConfig = execute._model_config

    dest_env = dest_env or os.environ.get("SIDEWINDER_DEST_ENV")
    since = since or os.environ.get("SIDEWINDER_SINCE")
    until = until or os.environ.get("SIDEWINDER_UNTIL")
    mode = table_mode or TableMode(os.environ.get("SIDEWINDER_TABLE_MODE", config.default_table_mode.value))

    if not dest_env:
        raise ValueError("dest_env must be provided or set via SIDEWINDER_DEST_ENV")

    start_total = time.time()

    if verbose:
        date_range = f" ({since} to {until})" if since and until else ""
        print(f"[{datetime.now()}] Executing {config.name}{date_range}...")

    start_exec = time.time()
    df = execute(since=since, until=until, **kwargs)
    rows_count = len(df)
    exec_time = time.time() - start_exec

    if verbose:
        print(f"  ✓ {rows_count:,} rows in {exec_time:.2f}s")
        print(f"[{datetime.now()}] Writing to {config.destination_schema}.{config.destination_table}...")

    start_write = time.time()
    write(
        dest_env=dest_env,
        df=df,
        schema=config.destination_schema,
        table=config.destination_table,
        table_mode=mode.value,
        ddl=config.destination_ddl,
        since=since,
        until=until,
    )

    create_indexes(
        dest_env=dest_env,
        schema=config.destination_schema,
        table=config.destination_table,
        indexes=config.destination_indexes,
    )

    write_time = time.time() - start_write
    total_time = time.time() - start_total

    if verbose:
        print(f"  ✓ Written in {write_time:.2f}s")
        print(f"  Total: {total_time:.2f}s")

    return {
        "rows": rows_count,
        "exec_time": exec_time,
        "write_time": write_time,
        "total_time": total_time,
    }
