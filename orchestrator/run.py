import time
import polars as pl
from datetime import datetime
from enum import Enum
from typing import Callable, Optional


class TableMode(Enum):
    FULL = "replace"
    INCREMENTAL = "append"


def run_ingest(
    dest_env: str,
    dest_table: str,
    execute: Callable,
    table_mode: TableMode = TableMode.FULL,
    schedule: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    verbose: bool = True,
    force: bool = False,
    **kwargs,
) -> dict:
    from orchestrator.write import write, write_polars
    from orchestrator.cron_checker import CronChecker

    checker = None
    if schedule:
        checker = CronChecker(model_name=dest_table, schedule=schedule)
        checker.check_and_start(force=force)

    start_total = time.time()

    try:
        if verbose:
            date_range = f" ({start} to {end})" if start and end else ""
            print(f"[{datetime.now()}] Executing {dest_table}{date_range}...")

        start_exec = time.time()
        result = execute(start=start, end=end, **kwargs)

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

        result_dict = {
            "rows": rows_count,
            "exec_time": exec_time,
            "write_time": write_time,
            "total_time": total_time,
        }

        if checker:
            checker.update_success(rows_count, total_time)

        return result_dict

    except Exception as e:
        total_time = time.time() - start_total
        if checker:
            checker.update_failure(str(e), total_time)
        raise
