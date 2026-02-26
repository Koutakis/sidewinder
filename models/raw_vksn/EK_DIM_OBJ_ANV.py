from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_anv",
    source_entity="EK_DIM_OBJ_ANV",
    table="ek_dim_obj_anv",
    schema="raindance_raw_1550",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANV_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="GRUPP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GRUPP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GRUPP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="GRUPP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="GRUPP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="GRUPP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['vksn', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([ANV_GILTIG_FOM], '1899-12-31 00:00:00') AS ANV_GILTIG_FOM,
	COALESCE([ANV_GILTIG_TOM], '1899-12-31 00:00:00') AS ANV_GILTIG_TOM,
	[ANV_ID] AS ANV_ID,
	[ANV_ID_TEXT] AS ANV_ID_TEXT,
	[ANV_PASSIV] AS ANV_PASSIV,
	[ANV_TEXT] AS ANV_TEXT,
	COALESCE([GRUPP_GILTIG_FOM], '1899-12-31 00:00:00') AS GRUPP_GILTIG_FOM,
	COALESCE([GRUPP_GILTIG_TOM], '1899-12-31 00:00:00') AS GRUPP_GILTIG_TOM,
	[GRUPP_ID] AS GRUPP_ID,
	[GRUPP_ID_TEXT] AS GRUPP_ID_TEXT,
	[GRUPP_PASSIV] AS GRUPP_PASSIV,
	[GRUPP_TEXT] AS GRUPP_TEXT
    FROM [utdata].[utdata155].[EK_DIM_OBJ_ANV]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_1550", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")