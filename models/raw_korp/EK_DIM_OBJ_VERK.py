from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_verk",
    source_entity="EK_DIM_OBJ_VERK",
    table="ek_dim_obj_verk",
    schema="raindance_raw_2870",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VERK_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VGREN_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VGREN_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VGREN_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['korp', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([VERK_GILTIG_FOM], '1899-12-31 00:00:00') AS VERK_GILTIG_FOM,
	COALESCE([VERK_GILTIG_TOM], '1899-12-31 00:00:00') AS VERK_GILTIG_TOM,
	[VERK_ID] AS VERK_ID,
	[VERK_ID_TEXT] AS VERK_ID_TEXT,
	[VERK_PASSIV] AS VERK_PASSIV,
	[VERK_TEXT] AS VERK_TEXT,
	COALESCE([VGREN_GILTIG_FOM], '1899-12-31 00:00:00') AS VGREN_GILTIG_FOM,
	COALESCE([VGREN_GILTIG_TOM], '1899-12-31 00:00:00') AS VGREN_GILTIG_TOM,
	[VGREN_ID] AS VGREN_ID,
	[VGREN_ID_TEXT] AS VGREN_ID_TEXT,
	[VGREN_PASSIV] AS VGREN_PASSIV,
	[VGREN_TEXT] AS VGREN_TEXT
    FROM [utdata].[utdata287].[EK_DIM_OBJ_VERK]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2870", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")