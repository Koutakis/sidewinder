from dataclasses import dataclass, field
from enum import Enum
from typing import Callable
import polars as pl


class TableMode(Enum):
    APPEND = "append"
    TRUNCATE_INSERT = "truncate_insert"
    MERGE = "merge"


@dataclass
class ModelConfig:
    name: str
    source_table: str
    destination_table: str
    destination_schema: str
    destination_ddl: str | None = None
    destination_indexes: list[str] = field(default_factory=list)
    default_table_mode: TableMode = TableMode.APPEND


def model(
    name: str,
    source_table: str,
    destination_table: str,
    destination_schema: str,
    destination_ddl: str | None = None,
    destination_indices: list[str] | None = None,
    default_table_mode: TableMode = TableMode.APPEND,
) -> Callable:
    config = ModelConfig(
        name=name,
        source_table=source_table,
        destination_table=destination_table,
        destination_schema=destination_schema,
        destination_ddl=destination_ddl,
        destination_indexes=destination_indices or [],
        default_table_mode=default_table_mode,
    )

    def decorator(fn: Callable[..., pl.DataFrame]) -> Callable[..., pl.DataFrame]:
        fn._model_config = config
        return fn

    return decorator
