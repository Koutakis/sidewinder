from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_konto",
    source_entity="EK_DIM_OBJ_KONTO",
    table="ek_dim_obj_konto",
    schema="raindance_raw_2920",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRA01_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRA01_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRA01_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRA01_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRA01_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRA01_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KONTO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KONTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KONTO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TSIK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TSIK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TSIK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TSIK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TSIK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="TSIK_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['rk', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([FRA01_GILTIG_FOM], '1899-12-31 00:00:00') AS FRA01_GILTIG_FOM,
	COALESCE([FRA01_GILTIG_TOM], '1899-12-31 00:00:00') AS FRA01_GILTIG_TOM,
	[FRA01_ID] AS FRA01_ID,
	[FRA01_ID_TEXT] AS FRA01_ID_TEXT,
	[FRA01_PASSIV] AS FRA01_PASSIV,
	[FRA01_TEXT] AS FRA01_TEXT,
	COALESCE([KONTO_GILTIG_FOM], '1899-12-31 00:00:00') AS KONTO_GILTIG_FOM,
	COALESCE([KONTO_GILTIG_TOM], '1899-12-31 00:00:00') AS KONTO_GILTIG_TOM,
	[KONTO_ID] AS KONTO_ID,
	[KONTO_ID_TEXT] AS KONTO_ID_TEXT,
	[KONTO_PASSIV] AS KONTO_PASSIV,
	[KONTO_TEXT] AS KONTO_TEXT,
	COALESCE([TSIK_GILTIG_FOM], '1899-12-31 00:00:00') AS TSIK_GILTIG_FOM,
	COALESCE([TSIK_GILTIG_TOM], '1899-12-31 00:00:00') AS TSIK_GILTIG_TOM,
	[TSIK_ID] AS TSIK_ID,
	[TSIK_ID_TEXT] AS TSIK_ID_TEXT,
	[TSIK_PASSIV] AS TSIK_PASSIV,
	[TSIK_TEXT] AS TSIK_TEXT
    FROM [utdata].[utdata292].[EK_DIM_OBJ_KONTO]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2920", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")