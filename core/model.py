from dataclasses import dataclass, field
from enum import Enum


class TableMode(Enum):
    APPEND = "APPEND"
    TRUNCATE_INSERT = "TRUNCATE_INSERT"
    MERGE = "MERGE"


@dataclass
class ModelConfig:
    name: str
    source_table: str
    destination_table: str = ""
    destination_schema: str = ""
 # destination_ddl: str | None = None
    columns: list[dict]
    destination_indexes: list[str] = field(default_factory=list)
    dest_env: str | None = None
    table_mode: TableMode = TableMode.APPEND
    tags: list[str]= None
    enabled: bool = False #Test this
