from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_kgrr",
    source_entity="EK_DIM_OBJ_KGRR",
    table="ek_dim_obj_kgrr",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KGRR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KGRR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KGRR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KGRR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KGRR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KGRR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKLRR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KKLRR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KKLRR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKLRR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KKLRR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KKLRR_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([KGRR_GILTIG_FOM], '1899-12-31 00:00:00') AS KGRR_GILTIG_FOM,
	COALESCE([KGRR_GILTIG_TOM], '1899-12-31 00:00:00') AS KGRR_GILTIG_TOM,
	[KGRR_ID] AS KGRR_ID,
	[KGRR_ID_TEXT] AS KGRR_ID_TEXT,
	[KGRR_PASSIV] AS KGRR_PASSIV,
	[KGRR_TEXT] AS KGRR_TEXT,
	COALESCE([KKLRR_GILTIG_FOM], '1899-12-31 00:00:00') AS KKLRR_GILTIG_FOM,
	COALESCE([KKLRR_GILTIG_TOM], '1899-12-31 00:00:00') AS KKLRR_GILTIG_TOM,
	[KKLRR_ID] AS KKLRR_ID,
	[KKLRR_ID_TEXT] AS KKLRR_ID_TEXT,
	[KKLRR_PASSIV] AS KKLRR_PASSIV,
	[KKLRR_TEXT] AS KKLRR_TEXT
    FROM [utdata].[utdata298].[EK_DIM_OBJ_KGRR]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2985", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")