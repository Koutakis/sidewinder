from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_anvgrp",
    source_entity="EK_DIM_OBJ_ANVGRP",
    table="ek_dim_obj_anvgrp",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANVGRP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANVGRP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANVGRP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANVGRP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANVGRP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANVGRP_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTCE_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAKTCE_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAKTCE_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTCE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTCE_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FAKTCE_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([ANVGRP_GILTIG_FOM], '1899-12-31 00:00:00') AS ANVGRP_GILTIG_FOM,
	COALESCE([ANVGRP_GILTIG_TOM], '1899-12-31 00:00:00') AS ANVGRP_GILTIG_TOM,
	[ANVGRP_ID] AS ANVGRP_ID,
	[ANVGRP_ID_TEXT] AS ANVGRP_ID_TEXT,
	[ANVGRP_PASSIV] AS ANVGRP_PASSIV,
	[ANVGRP_TEXT] AS ANVGRP_TEXT,
	COALESCE([FAKTCE_GILTIG_FOM], '1899-12-31 00:00:00') AS FAKTCE_GILTIG_FOM,
	COALESCE([FAKTCE_GILTIG_TOM], '1899-12-31 00:00:00') AS FAKTCE_GILTIG_TOM,
	[FAKTCE_ID] AS FAKTCE_ID,
	[FAKTCE_ID_TEXT] AS FAKTCE_ID_TEXT,
	[FAKTCE_PASSIV] AS FAKTCE_PASSIV,
	[FAKTCE_TEXT] AS FAKTCE_TEXT
    FROM [udpb4].[udpb4_100].[EK_DIM_OBJ_ANVGRP]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_1100", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")