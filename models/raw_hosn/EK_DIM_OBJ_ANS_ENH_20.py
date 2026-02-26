from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_ans_enh_20",
    source_entity="EK_DIM_OBJ_ANS_ENH_20",
    table="ek_dim_obj_ans_enh_20",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANS_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANS_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANS_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANS_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANS_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANS_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVD_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVD_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVD_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="AVD_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENH_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENH_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ENH_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FTG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FTG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FTG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FTG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FTG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FTG_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RAM_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RAM_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RAM_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RAM_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RAM_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="RAM_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([ANS_GILTIG_FOM], '1899-12-31 00:00:00') AS ANS_GILTIG_FOM,
	COALESCE([ANS_GILTIG_TOM], '1899-12-31 00:00:00') AS ANS_GILTIG_TOM,
	[ANS_ID] AS ANS_ID,
	[ANS_ID_TEXT] AS ANS_ID_TEXT,
	[ANS_PASSIV] AS ANS_PASSIV,
	[ANS_TEXT] AS ANS_TEXT,
	COALESCE([AVD_GILTIG_FOM], '1899-12-31 00:00:00') AS AVD_GILTIG_FOM,
	COALESCE([AVD_GILTIG_TOM], '1899-12-31 00:00:00') AS AVD_GILTIG_TOM,
	[AVD_ID] AS AVD_ID,
	[AVD_ID_TEXT] AS AVD_ID_TEXT,
	[AVD_PASSIV] AS AVD_PASSIV,
	[AVD_TEXT] AS AVD_TEXT,
	COALESCE([ENH_GILTIG_FOM], '1899-12-31 00:00:00') AS ENH_GILTIG_FOM,
	COALESCE([ENH_GILTIG_TOM], '1899-12-31 00:00:00') AS ENH_GILTIG_TOM,
	[ENH_ID] AS ENH_ID,
	[ENH_ID_TEXT] AS ENH_ID_TEXT,
	[ENH_PASSIV] AS ENH_PASSIV,
	[ENH_TEXT] AS ENH_TEXT,
	COALESCE([FTG_GILTIG_FOM], '1899-12-31 00:00:00') AS FTG_GILTIG_FOM,
	COALESCE([FTG_GILTIG_TOM], '1899-12-31 00:00:00') AS FTG_GILTIG_TOM,
	[FTG_ID] AS FTG_ID,
	[FTG_ID_TEXT] AS FTG_ID_TEXT,
	[FTG_PASSIV] AS FTG_PASSIV,
	[FTG_TEXT] AS FTG_TEXT,
	COALESCE([RAM_GILTIG_FOM], '1899-12-31 00:00:00') AS RAM_GILTIG_FOM,
	COALESCE([RAM_GILTIG_TOM], '1899-12-31 00:00:00') AS RAM_GILTIG_TOM,
	[RAM_ID] AS RAM_ID,
	[RAM_ID_TEXT] AS RAM_ID_TEXT,
	[RAM_PASSIV] AS RAM_PASSIV,
	[RAM_TEXT] AS RAM_TEXT
    FROM [utdata].[utdata150].[EK_DIM_OBJ_ANS_ENH_20]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_1500", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")