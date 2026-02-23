from dataclasses import dataclass, field
from enum import Enum
from typing import Callable
import polars as pl
from roskarl.marshal import load_env_config
from core.write import write


class TableMode(Enum):
    APPEND = "append"
    TRUNCATE_INSERT = "truncate_insert"
    MERGE = "merge"


@dataclass
class ResolvedConfig:
    name: str
    source_table: str
    source_env: str | None
    destination_table: str
    destination_schema: str
    destination_ddl: str | None
    destination_indexes: list[str]
    dest_env: str | None
    table_mode: TableMode
    since: str | None
    until: str | None
    backfill_batch_size: int | None


def model(
    name: str,
    source_table: str,
    destination_table: str,
    destination_schema: str,
    destination_ddl: str | None = None,
    destination_indexes: list[str] | None = None,
    default_table_mode: TableMode = TableMode.APPEND,
    dest_env: str | None = None,
    source_env: str | None = None,
) -> Callable:

    def decorator(fn: Callable) -> Callable[..., None]:

        def wrapper(**kwargs) -> None:

            env = load_env_config()

            since = None
            until = None
            if env.cron and env.cron.since:
                since = str(env.cron.since)
                until = str(env.cron.until)
            elif env.backfill and env.backfill.since:
                since = str(env.backfill.since)
                until = str(env.backfill.until) if env.backfill.until else None

            cfg = ResolvedConfig(
                name=name,
                source_table=source_table,
                source_env=source_env,
                destination_table=destination_table,
                destination_schema=destination_schema,
                destination_ddl=destination_ddl,
                destination_indexes=destination_indexes or [],
                dest_env=dest_env,
                table_mode=default_table_mode,
                since=since,
                until=until,
                backfill_batch_size=env.backfill.batch_size if env.backfill else None,
            )

            if cfg.table_mode == TableMode.MERGE and not cfg.since:
                raise ValueError(f"{cfg.name}: MERGE mode requires 'since'")

            if not cfg.dest_env:
                raise ValueError(f"{cfg.name}: dest_env must be set")

            total_rows = 0
            first_batch = True

            for df in fn(cfg, **kwargs):
                if len(df) == 0:
                    continue

                if first_batch:
                    write(cfg, df)
                    first_batch = False
                    cfg.table_mode = TableMode.APPEND
                else:
                    write(cfg, df)

                total_rows += len(df)

            if total_rows == 0:
                print(f"  ⏭ {cfg.name}: no data, skipping")
            else:
                print(f"  ✓ {cfg.name}: {total_rows:,} rows written")

        return wrapper

    return decorator
