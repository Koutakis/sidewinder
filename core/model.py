from dataclasses import dataclass, field
from enum import Enum


class TableMode(Enum):
    APPEND = "append"
    TRUNCATE_INSERT = "truncate_insert"
    MERGE = "merge"


@dataclass
class ModelConfig:
    name: str
    source_table: str
    source_env: str | None = None
    destination_table: str = ""
    destination_schema: str = ""
    destination_ddl: str | None = None
    destination_indexes: list[str] = field(default_factory=list)
    dest_env: str | None = None
    table_mode: TableMode = TableMode.APPEND
