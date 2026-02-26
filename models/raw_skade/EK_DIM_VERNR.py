from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_vernr",
    source_entity="EK_DIM_VERNR",
    table="ek_dim_vernr",
    schema="raindance_raw_2990",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AR", data_type=PostgresType.TEXT),
        PostgresColumn(name="INTERNVERNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="INTERNVERNR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERNRGRUPP", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['skade', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	[AR] AS AR,
	[INTERNVERNR] AS INTERNVERNR,
	[INTERNVERNR_TEXT] AS INTERNVERNR_TEXT,
	COALESCE([VERDATUM], '1899-12-31 00:00:00') AS VERDATUM,
	[VERNRGRUPP] AS VERNRGRUPP
    FROM [utdata].[utdata299].[EK_DIM_VERNR]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2990", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")