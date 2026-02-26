from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_yrke",
    source_entity="EK_DIM_OBJ_YRKE",
    table="ek_dim_obj_yrke",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SJKYRK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SJKYRK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SJKYRK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SJKYRK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SJKYRK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SJKYRK_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="YRKE_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="YRKE_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="YRKE_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="YRKE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="YRKE_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="YRKE_TEXT", data_type=PostgresType.TEXT),
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
	COALESCE([SJKYRK_GILTIG_FOM], '1899-12-31 00:00:00') AS SJKYRK_GILTIG_FOM,
	COALESCE([SJKYRK_GILTIG_TOM], '1899-12-31 00:00:00') AS SJKYRK_GILTIG_TOM,
	[SJKYRK_ID] AS SJKYRK_ID,
	[SJKYRK_ID_TEXT] AS SJKYRK_ID_TEXT,
	[SJKYRK_PASSIV] AS SJKYRK_PASSIV,
	[SJKYRK_TEXT] AS SJKYRK_TEXT,
	COALESCE([YRKE_GILTIG_FOM], '1899-12-31 00:00:00') AS YRKE_GILTIG_FOM,
	COALESCE([YRKE_GILTIG_TOM], '1899-12-31 00:00:00') AS YRKE_GILTIG_TOM,
	[YRKE_ID] AS YRKE_ID,
	[YRKE_ID_TEXT] AS YRKE_ID_TEXT,
	[YRKE_PASSIV] AS YRKE_PASSIV,
	[YRKE_TEXT] AS YRKE_TEXT
    FROM [Utdata].[udp_100].[EK_DIM_OBJ_YRKE]

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