# Sidewinder

Data ingestion framework. Writes to PostgreSQL using psycopg COPY. Source-agnostic — the execute function can return data from anywhere as long as it yields Polars DataFrames.

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
python main.py
```

Discovers and runs all models in `models/`.

### Filter by model

```bash
MODELS="ek_fakta_verifikat,rk_dim_utility" python main.py
```

### Backfill

```bash
BACKFILL_ENABLED=true BACKFILL_SINCE=2024-01-01T00:00:00Z BACKFILL_UNTIL=2024-06-01T00:00:00Z python main.py
```

### Cron mode

```bash
CRON_ENABLED=true CRON_EXPRESSION="0 6 * * *" python main.py
```

`since` and `until` are derived from the last fully elapsed interval.

## Add a model

Create a file in `models/`, e.g. `models/raw_nks/my_table.py`:

```python
from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read


config = Model(
    name="my_table",
    source_entity="MY_TABLE",
    table="my_table",
    schema="my_schema",
    write_mode=WriteMode.TRUNCATE_INSERT,
    database=Database.POSTGRES,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="id", data_type=PostgresType.BIGINT),
        PostgresColumn(name="name", data_type=PostgresType.TEXT),
    ],
    cron="0 6 * * *",
    tags=["my_tag"],
)


def execute(env, cfg=config):
    query = """
    SELECT
        CAST(GETDATE() AS DATE) as _data_modified,
        [ID] AS id,
        [NAME] AS name
    FROM [db].[schema].[MY_TABLE]
    """
    yield from read("SOURCE_ENV_NAME", query)
```

For MERGE models, resolve `since`/`until` from env:

```python
def execute(env, cfg=config):
    if env.backfill and env.backfill.enabled:
        since = env.backfill.since.strftime("%Y-%m-%d")
        until = env.backfill.until.strftime("%Y-%m-%d")
    elif env.cron and env.cron.enabled:
        since = env.cron.since.strftime("%Y-%m-%d")
        until = env.cron.until.strftime("%Y-%m-%d")
    else:
        raise ValueError("MERGE requires CRON or BACKFILL env vars")

    query = f"""
    SELECT
        CAST(VERDATUM AS DATE) as _data_modified,
        [ID] AS id
    FROM [db].[schema].[MY_TABLE]
    WHERE CAST(VERDATUM AS DATE) BETWEEN '{since}' AND '{until}'
    """
    yield from read("SOURCE_ENV_NAME", query)
```

## Write modes

| Mode | Behavior |
|---|---|
| `APPEND` | Insert rows |
| `TRUNCATE_INSERT` | Truncate table, then insert |
| `MERGE` | Delete `[since, until)` range, then insert |
| `VIEW` | Create or replace view |

## Env vars

| Var | Description |
|---|---|
| `DEST_ENV` | Postgres destination DSN name |
| `MODELS` | Comma-separated model names to run |
| `TAGS` | Comma-separated tags to filter by |
| `CRON_ENABLED` | Enable cron mode |
| `CRON_EXPRESSION` | Cron expression |
| `BACKFILL_ENABLED` | Enable backfill mode |
| `BACKFILL_SINCE` | ISO8601 UTC datetime |
| `BACKFILL_UNTIL` | ISO8601 UTC datetime |

## Project structure

```
sidewinder/
├── config/
│   ├── connections.py
│   └── type_mapping.py
├── core/
│   ├── read.py
│   ├── write.py
│   ├── run.py
│   └── logger.py
├── models/
├── main.py
└── requirements.txt
```
