from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_defanl",
    source_entity="EK_DIM_OBJ_DEFANL",
    table="ek_dim_obj_defanl",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DEFANL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DEFANL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DEFANL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="DEFANL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DEFANL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="DEFANL_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([DEFANL_GILTIG_FOM], '1899-12-31 00:00:00') AS DEFANL_GILTIG_FOM,
	COALESCE([DEFANL_GILTIG_TOM], '1899-12-31 00:00:00') AS DEFANL_GILTIG_TOM,
	[DEFANL_ID] AS DEFANL_ID,
	[DEFANL_ID_TEXT] AS DEFANL_ID_TEXT,
	[DEFANL_PASSIV] AS DEFANL_PASSIV,
	[DEFANL_TEXT] AS DEFANL_TEXT
    FROM [raindance_udp].[udp_150].[EK_DIM_OBJ_DEFANL]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8510", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")