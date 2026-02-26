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
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KST_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEKT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SEKT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SEKT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEKT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SEKT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SEKT_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="V_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="V_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="V_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="V_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="V_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="V_TEXT", data_type=PostgresType.TEXT),
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
        PostgresColumn(name="VO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VO_TEXT", data_type=PostgresType.TEXT),
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
	COALESCE([KST_GILTIG_FOM], '1899-12-31 00:00:00') AS KST_GILTIG_FOM,
	COALESCE([KST_GILTIG_TOM], '1899-12-31 00:00:00') AS KST_GILTIG_TOM,
	[KST_ID] AS KST_ID,
	[KST_ID_TEXT] AS KST_ID_TEXT,
	[KST_PASSIV] AS KST_PASSIV,
	[KST_TEXT] AS KST_TEXT,
	COALESCE([SEKT_GILTIG_FOM], '1899-12-31 00:00:00') AS SEKT_GILTIG_FOM,
	COALESCE([SEKT_GILTIG_TOM], '1899-12-31 00:00:00') AS SEKT_GILTIG_TOM,
	[SEKT_ID] AS SEKT_ID,
	[SEKT_ID_TEXT] AS SEKT_ID_TEXT,
	[SEKT_PASSIV] AS SEKT_PASSIV,
	[SEKT_TEXT] AS SEKT_TEXT,
	COALESCE([V_GILTIG_FOM], '1899-12-31 00:00:00') AS V_GILTIG_FOM,
	COALESCE([V_GILTIG_TOM], '1899-12-31 00:00:00') AS V_GILTIG_TOM,
	[V_ID] AS V_ID,
	[V_ID_TEXT] AS V_ID_TEXT,
	[V_PASSIV] AS V_PASSIV,
	[V_TEXT] AS V_TEXT,
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
	[VGREN_TEXT] AS VGREN_TEXT,
	COALESCE([VO_GILTIG_FOM], '1899-12-31 00:00:00') AS VO_GILTIG_FOM,
	COALESCE([VO_GILTIG_TOM], '1899-12-31 00:00:00') AS VO_GILTIG_TOM,
	[VO_ID] AS VO_ID,
	[VO_ID_TEXT] AS VO_ID_TEXT,
	[VO_PASSIV] AS VO_PASSIV,
	[VO_TEXT] AS VO_TEXT
    FROM [raindance_udp].[udp_150].[EK_DIM_OBJ_KST]

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