from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ar_dim_obj_kst",
    source_entity="AR_DIM_OBJ_KST",
    table="ar_dim_obj_kst",
    schema="raindance_raw_8570",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSVAR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSVAR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSVAR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSVAR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSVAR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANSVAR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FOUU_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FOUU_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FOUU_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FOUU_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FOUU_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FOUU_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FUNK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FUNK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FUNK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FUNK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FUNK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FUNK_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KSG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KSG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KSG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KSG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KSG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KSG_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KST_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MKST_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MKST_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MKST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MKST_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MKST_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MKST_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SLLVGR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SLLVGR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SLLVGR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SLLVGR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SLLVGR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SLLVGR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VKO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VKO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VKO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VKO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VKO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VKO_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sos', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([ANSVAR_GILTIG_FOM], '1899-12-31 00:00:00') AS ANSVAR_GILTIG_FOM,
	COALESCE([ANSVAR_GILTIG_TOM], '1899-12-31 00:00:00') AS ANSVAR_GILTIG_TOM,
	[ANSVAR_ID] AS ANSVAR_ID,
	[ANSVAR_ID_TEXT] AS ANSVAR_ID_TEXT,
	[ANSVAR_PASSIV] AS ANSVAR_PASSIV,
	[ANSVAR_TEXT] AS ANSVAR_TEXT,
	COALESCE([FOUU_GILTIG_FOM], '1899-12-31 00:00:00') AS FOUU_GILTIG_FOM,
	COALESCE([FOUU_GILTIG_TOM], '1899-12-31 00:00:00') AS FOUU_GILTIG_TOM,
	[FOUU_ID] AS FOUU_ID,
	[FOUU_ID_TEXT] AS FOUU_ID_TEXT,
	[FOUU_PASSIV] AS FOUU_PASSIV,
	[FOUU_TEXT] AS FOUU_TEXT,
	COALESCE([FUNK_GILTIG_FOM], '1899-12-31 00:00:00') AS FUNK_GILTIG_FOM,
	COALESCE([FUNK_GILTIG_TOM], '1899-12-31 00:00:00') AS FUNK_GILTIG_TOM,
	[FUNK_ID] AS FUNK_ID,
	[FUNK_ID_TEXT] AS FUNK_ID_TEXT,
	[FUNK_PASSIV] AS FUNK_PASSIV,
	[FUNK_TEXT] AS FUNK_TEXT,
	COALESCE([KSG_GILTIG_FOM], '1899-12-31 00:00:00') AS KSG_GILTIG_FOM,
	COALESCE([KSG_GILTIG_TOM], '1899-12-31 00:00:00') AS KSG_GILTIG_TOM,
	[KSG_ID] AS KSG_ID,
	[KSG_ID_TEXT] AS KSG_ID_TEXT,
	[KSG_PASSIV] AS KSG_PASSIV,
	[KSG_TEXT] AS KSG_TEXT,
	COALESCE([KST_GILTIG_FOM], '1899-12-31 00:00:00') AS KST_GILTIG_FOM,
	COALESCE([KST_GILTIG_TOM], '1899-12-31 00:00:00') AS KST_GILTIG_TOM,
	[KST_ID] AS KST_ID,
	[KST_ID_TEXT] AS KST_ID_TEXT,
	[KST_PASSIV] AS KST_PASSIV,
	[KST_TEXT] AS KST_TEXT,
	COALESCE([MKST_GILTIG_FOM], '1899-12-31 00:00:00') AS MKST_GILTIG_FOM,
	COALESCE([MKST_GILTIG_TOM], '1899-12-31 00:00:00') AS MKST_GILTIG_TOM,
	[MKST_ID] AS MKST_ID,
	[MKST_ID_TEXT] AS MKST_ID_TEXT,
	[MKST_PASSIV] AS MKST_PASSIV,
	[MKST_TEXT] AS MKST_TEXT,
	COALESCE([SLLVGR_GILTIG_FOM], '1899-12-31 00:00:00') AS SLLVGR_GILTIG_FOM,
	COALESCE([SLLVGR_GILTIG_TOM], '1899-12-31 00:00:00') AS SLLVGR_GILTIG_TOM,
	[SLLVGR_ID] AS SLLVGR_ID,
	[SLLVGR_ID_TEXT] AS SLLVGR_ID_TEXT,
	[SLLVGR_PASSIV] AS SLLVGR_PASSIV,
	[SLLVGR_TEXT] AS SLLVGR_TEXT,
	COALESCE([VKO_GILTIG_FOM], '1899-12-31 00:00:00') AS VKO_GILTIG_FOM,
	COALESCE([VKO_GILTIG_TOM], '1899-12-31 00:00:00') AS VKO_GILTIG_TOM,
	[VKO_ID] AS VKO_ID,
	[VKO_ID_TEXT] AS VKO_ID_TEXT,
	[VKO_PASSIV] AS VKO_PASSIV,
	[VKO_TEXT] AS VKO_TEXT
    FROM [raindance_udp].[udp_220].[AR_DIM_OBJ_KST]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8570", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")