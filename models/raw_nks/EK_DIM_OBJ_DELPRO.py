from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_delpro",
    source_entity="EK_DIM_OBJ_DELPRO",
    table="ek_dim_obj_delpro",
    schema="raindance_raw_2710",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DELPRO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DELPRO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DELPRO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="DELPRO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DELPRO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="DELPRO_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TPROJ_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TPROJ_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TPROJ_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TPROJ_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TPROJ_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="TPROJ_TEXT", data_type=PostgresType.TEXT),
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
	COALESCE([DELPRO_GILTIG_FOM], '1899-12-31 00:00:00') AS DELPRO_GILTIG_FOM,
	COALESCE([DELPRO_GILTIG_TOM], '1899-12-31 00:00:00') AS DELPRO_GILTIG_TOM,
	[DELPRO_ID] AS DELPRO_ID,
	[DELPRO_ID_TEXT] AS DELPRO_ID_TEXT,
	[DELPRO_PASSIV] AS DELPRO_PASSIV,
	[DELPRO_TEXT] AS DELPRO_TEXT,
	COALESCE([TPROJ_GILTIG_FOM], '1899-12-31 00:00:00') AS TPROJ_GILTIG_FOM,
	COALESCE([TPROJ_GILTIG_TOM], '1899-12-31 00:00:00') AS TPROJ_GILTIG_TOM,
	[TPROJ_ID] AS TPROJ_ID,
	[TPROJ_ID_TEXT] AS TPROJ_ID_TEXT,
	[TPROJ_PASSIV] AS TPROJ_PASSIV,
	[TPROJ_TEXT] AS TPROJ_TEXT
    FROM [raindance_udp].[udp_100].[EK_DIM_OBJ_DELPRO]

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