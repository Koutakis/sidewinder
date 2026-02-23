# Sidewinder

Data ingestion framework. Reads from MSSQL, writes to PostgreSQL using psycopg COPY.

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
python main.py
```

Discovers and runs all models in `models/`.

## Add a model

Create a file in `models/`, e.g. `models/my_source/my_table.py`:

```python
from core import ModelConfig, TableMode, read


config = ModelConfig(
    name="my_table",
    source_table="MY_TABLE",
    source_env="SOURCE_DSN_NAME",
    destination_table="my_table",
    destination_schema="my_schema",
    dest_env="DEST_DSN_NAME",
    table_mode=TableMode.TRUNCATE_INSERT,
)


def execute(env, cfg=config):
    query = """
    SELECT col1, col2, col3
    FROM [db].[schema].[MY_TABLE]
    """
    yield from read(cfg.source_env, query)
```

Add `__init__.py` to any new subdirectory:

```bash
find models -type d -exec touch {}/__init__.py \;
```

## Table modes

| Mode | Behavior |
|---|---|
| `APPEND` | Insert rows, no delete |
| `TRUNCATE_INSERT` | Truncate table, then insert |
| `MERGE` | Delete rows in `since`/`until` range, then insert |

## Env vars

Set by the orchestrator (Airflow/k3s). Read via [roskarl](https://github.com/ebremstedt/roskarl).

| Var | Description |
|---|---|
| `CRON_ENABLED` | Enable cron mode |
| `CRON_EXPRESSION` | Cron expression (derives `since`/`until`) |
| `BACKFILL_ENABLED` | Enable backfill mode |
| `BACKFILL_SINCE` | ISO8601 datetime |
| `BACKFILL_UNTIL` | ISO8601 datetime |

## Project structure

```
sidewinder/
├── config/
│   ├── connections.py     # MSSQL + Postgres connection builders
│   └── type_mapping.py    # Polars → Postgres type mapping
├── core/
│   ├── model.py           # ModelConfig + TableMode
│   ├── read.py            # Batched MSSQL reader (Generator)
│   ├── write.py           # Postgres writer (psycopg COPY)
│   ├── run.py             # Batch iterator + write orchestration
│   └── logger.py          # Print helpers
├── models/                # Your models go here
├── main.py                # Entrypoint
└── requirements.txt
```
