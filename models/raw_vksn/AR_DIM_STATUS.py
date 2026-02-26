from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ar_dim_status",
    source_entity="AR_DIM_STATUS",
    table="ar_dim_status",
    schema="raindance_raw_1550",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANLSTATUS", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANLSTATUS2", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANLSTATUS2_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANLSTATUS_TEXT", data_type=PostgresType.TEXT),
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
	[ANLSTATUS] AS ANLSTATUS,
	[ANLSTATUS2] AS ANLSTATUS2,
	[ANLSTATUS2_TEXT] AS ANLSTATUS2_TEXT,
	[ANLSTATUS_TEXT] AS ANLSTATUS_TEXT
    FROM [utdata].[utdata155].[AR_DIM_STATUS]

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