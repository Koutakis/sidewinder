from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_uppkod",
    source_entity="EK_DIM_OBJ_UPPKOD",
    table="ek_dim_obj_uppkod",
    schema="raindance_raw_8530",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UPPKOD_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UPPKOD_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UPPKOD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="UPPKOD_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="UPPKOD_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="UPPKOD_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ste', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([UPPKOD_GILTIG_FOM], '1899-12-31 00:00:00') AS UPPKOD_GILTIG_FOM,
	COALESCE([UPPKOD_GILTIG_TOM], '1899-12-31 00:00:00') AS UPPKOD_GILTIG_TOM,
	[UPPKOD_ID] AS UPPKOD_ID,
	[UPPKOD_ID_TEXT] AS UPPKOD_ID_TEXT,
	[UPPKOD_PASSIV] AS UPPKOD_PASSIV,
	[UPPKOD_TEXT] AS UPPKOD_TEXT
    FROM [steudp].[udp_600].[EK_DIM_OBJ_UPPKOD]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8530", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")