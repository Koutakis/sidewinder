from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="rk_dim_status",
    source_entity="RK_DIM_STATUS",
    table="rk_dim_status",
    schema="raindance_raw_2880",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAKTSTATUS", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTSTATUS_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTSTATUSTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTSTATUSTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['khn', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[FAKTSTATUS] AS FAKTSTATUS,
	[FAKTSTATUS_TEXT] AS FAKTSTATUS_TEXT,
	[FAKTSTATUSTYP] AS FAKTSTATUSTYP,
	[FAKTSTATUSTYP_TEXT] AS FAKTSTATUSTYP_TEXT
    FROM [utdata].[utdata288].[RK_DIM_STATUS]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2880", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")