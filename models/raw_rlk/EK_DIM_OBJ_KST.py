from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_kst",
    source_entity="EK_DIM_OBJ_KST",
    table="ek_dim_obj_kst",
    schema="raindance_raw_2950",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVD_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVD_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="AVD_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVDEL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVDEL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVDEL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVDEL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVDEL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="AVDEL_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DIR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DIR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DIR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="DIR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DIR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="DIR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENH_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENH_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ENH_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENHET_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENHET_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENHET_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENHET_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENHET_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ENHET_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KST_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="RL_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEKT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SEKT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SEKT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEKT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEKT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SEKT_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEKTN_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SEKTN_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SEKTN_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEKTN_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEKTN_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SEKTN_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['rlk', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([AVD_GILTIG_FOM], '1899-12-31 00:00:00') AS AVD_GILTIG_FOM,
	COALESCE([AVD_GILTIG_TOM], '1899-12-31 00:00:00') AS AVD_GILTIG_TOM,
	[AVD_ID] AS AVD_ID,
	[AVD_ID_TEXT] AS AVD_ID_TEXT,
	[AVD_PASSIV] AS AVD_PASSIV,
	[AVD_TEXT] AS AVD_TEXT,
	COALESCE([AVDEL_GILTIG_FOM], '1899-12-31 00:00:00') AS AVDEL_GILTIG_FOM,
	COALESCE([AVDEL_GILTIG_TOM], '1899-12-31 00:00:00') AS AVDEL_GILTIG_TOM,
	[AVDEL_ID] AS AVDEL_ID,
	[AVDEL_ID_TEXT] AS AVDEL_ID_TEXT,
	[AVDEL_PASSIV] AS AVDEL_PASSIV,
	[AVDEL_TEXT] AS AVDEL_TEXT,
	COALESCE([DIR_GILTIG_FOM], '1899-12-31 00:00:00') AS DIR_GILTIG_FOM,
	COALESCE([DIR_GILTIG_TOM], '1899-12-31 00:00:00') AS DIR_GILTIG_TOM,
	[DIR_ID] AS DIR_ID,
	[DIR_ID_TEXT] AS DIR_ID_TEXT,
	[DIR_PASSIV] AS DIR_PASSIV,
	[DIR_TEXT] AS DIR_TEXT,
	COALESCE([ENH_GILTIG_FOM], '1899-12-31 00:00:00') AS ENH_GILTIG_FOM,
	COALESCE([ENH_GILTIG_TOM], '1899-12-31 00:00:00') AS ENH_GILTIG_TOM,
	[ENH_ID] AS ENH_ID,
	[ENH_ID_TEXT] AS ENH_ID_TEXT,
	[ENH_PASSIV] AS ENH_PASSIV,
	[ENH_TEXT] AS ENH_TEXT,
	COALESCE([ENHET_GILTIG_FOM], '1899-12-31 00:00:00') AS ENHET_GILTIG_FOM,
	COALESCE([ENHET_GILTIG_TOM], '1899-12-31 00:00:00') AS ENHET_GILTIG_TOM,
	[ENHET_ID] AS ENHET_ID,
	[ENHET_ID_TEXT] AS ENHET_ID_TEXT,
	[ENHET_PASSIV] AS ENHET_PASSIV,
	[ENHET_TEXT] AS ENHET_TEXT,
	COALESCE([KST_GILTIG_FOM], '1899-12-31 00:00:00') AS KST_GILTIG_FOM,
	COALESCE([KST_GILTIG_TOM], '1899-12-31 00:00:00') AS KST_GILTIG_TOM,
	[KST_ID] AS KST_ID,
	[KST_ID_TEXT] AS KST_ID_TEXT,
	[KST_PASSIV] AS KST_PASSIV,
	[KST_TEXT] AS KST_TEXT,
	COALESCE([RL_GILTIG_FOM], '1899-12-31 00:00:00') AS RL_GILTIG_FOM,
	COALESCE([RL_GILTIG_TOM], '1899-12-31 00:00:00') AS RL_GILTIG_TOM,
	[RL_ID] AS RL_ID,
	[RL_ID_TEXT] AS RL_ID_TEXT,
	[RL_PASSIV] AS RL_PASSIV,
	[RL_TEXT] AS RL_TEXT,
	COALESCE([SEKT_GILTIG_FOM], '1899-12-31 00:00:00') AS SEKT_GILTIG_FOM,
	COALESCE([SEKT_GILTIG_TOM], '1899-12-31 00:00:00') AS SEKT_GILTIG_TOM,
	[SEKT_ID] AS SEKT_ID,
	[SEKT_ID_TEXT] AS SEKT_ID_TEXT,
	[SEKT_PASSIV] AS SEKT_PASSIV,
	[SEKT_TEXT] AS SEKT_TEXT,
	COALESCE([SEKTN_GILTIG_FOM], '1899-12-31 00:00:00') AS SEKTN_GILTIG_FOM,
	COALESCE([SEKTN_GILTIG_TOM], '1899-12-31 00:00:00') AS SEKTN_GILTIG_TOM,
	[SEKTN_ID] AS SEKTN_ID,
	[SEKTN_ID_TEXT] AS SEKTN_ID_TEXT,
	[SEKTN_PASSIV] AS SEKTN_PASSIV,
	[SEKTN_TEXT] AS SEKTN_TEXT
    FROM [utdata].[utdata295].[EK_DIM_OBJ_KST]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_2950", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")