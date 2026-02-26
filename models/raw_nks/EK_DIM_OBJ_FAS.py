from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_fas",
    source_entity="EK_DIM_OBJ_FAS",
    table="ek_dim_obj_fas",
    schema="raindance_raw_2710",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAS_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAS_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAS_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAS_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAS_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FAS_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="GRFAS_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GRFAS_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GRFAS_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="GRFAS_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="GRFAS_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="GRFAS_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([FAS_GILTIG_FOM], '1899-12-31 00:00:00') AS FAS_GILTIG_FOM,
	COALESCE([FAS_GILTIG_TOM], '1899-12-31 00:00:00') AS FAS_GILTIG_TOM,
	[FAS_ID] AS FAS_ID,
	[FAS_ID_TEXT] AS FAS_ID_TEXT,
	[FAS_PASSIV] AS FAS_PASSIV,
	[FAS_TEXT] AS FAS_TEXT,
	COALESCE([GRFAS_GILTIG_FOM], '1899-12-31 00:00:00') AS GRFAS_GILTIG_FOM,
	COALESCE([GRFAS_GILTIG_TOM], '1899-12-31 00:00:00') AS GRFAS_GILTIG_TOM,
	[GRFAS_ID] AS GRFAS_ID,
	[GRFAS_ID_TEXT] AS GRFAS_ID_TEXT,
	[GRFAS_PASSIV] AS GRFAS_PASSIV,
	[GRFAS_TEXT] AS GRFAS_TEXT
    FROM [raindance_udp].[udp_100].[EK_DIM_OBJ_FAS]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2710", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")