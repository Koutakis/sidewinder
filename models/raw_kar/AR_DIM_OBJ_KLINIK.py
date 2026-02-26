from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ar_dim_obj_klinik",
    source_entity="AR_DIM_OBJ_KLINIK",
    table="ar_dim_obj_klinik",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DIV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DIV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DIV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="DIV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DIV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="DIV_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KLINIK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KLINIK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KLINIK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KLINIK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KLINIK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KLINIK_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([DIV_GILTIG_FOM], '1899-12-31 00:00:00') AS DIV_GILTIG_FOM,
	COALESCE([DIV_GILTIG_TOM], '1899-12-31 00:00:00') AS DIV_GILTIG_TOM,
	[DIV_ID] AS DIV_ID,
	[DIV_ID_TEXT] AS DIV_ID_TEXT,
	[DIV_PASSIV] AS DIV_PASSIV,
	[DIV_TEXT] AS DIV_TEXT,
	COALESCE([KLINIK_GILTIG_FOM], '1899-12-31 00:00:00') AS KLINIK_GILTIG_FOM,
	COALESCE([KLINIK_GILTIG_TOM], '1899-12-31 00:00:00') AS KLINIK_GILTIG_TOM,
	[KLINIK_ID] AS KLINIK_ID,
	[KLINIK_ID_TEXT] AS KLINIK_ID_TEXT,
	[KLINIK_PASSIV] AS KLINIK_PASSIV,
	[KLINIK_TEXT] AS KLINIK_TEXT
    FROM [Utdata].[udp_100].[AR_DIM_OBJ_KLINIK]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_1210", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")