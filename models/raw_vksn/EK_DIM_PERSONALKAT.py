from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_personalkat",
    source_entity="EK_DIM_PERSONALKAT",
    table="ek_dim_personalkat",
    schema="raindance_raw_1550",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PERSONALKAT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PERSONALKAT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PERSONALKAT_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['vksn', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[PERSONALKAT_ID] AS PERSONALKAT_ID,
	[PERSONALKAT_ID_TEXT] AS PERSONALKAT_ID_TEXT,
	[PERSONALKAT_TEXT] AS PERSONALKAT_TEXT
    FROM [utdata].[utdata155].[EK_DIM_PERSONALKAT]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_1550", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")