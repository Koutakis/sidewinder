from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_kst",
    source_entity="EK_DIM_OBJ_KST",
    table="ek_dim_obj_kst",
    schema="raindance_raw_nks",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FTG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FTG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FTG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FTG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FTG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FTG_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KST_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="OMR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OMR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OMR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="OMR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="OMR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="OMR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERKS_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERKS_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERKS_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERKS_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERKS_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VERKS_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([FTG_GILTIG_FOM], '1899-12-31 00:00:00') AS FTG_GILTIG_FOM,
    	COALESCE([FTG_GILTIG_TOM], '1899-12-31 00:00:00') AS FTG_GILTIG_TOM,
    	[FTG_ID] AS FTG_ID,
    	[FTG_ID_TEXT] AS FTG_ID_TEXT,
    	[FTG_PASSIV] AS FTG_PASSIV,
    	[FTG_TEXT] AS FTG_TEXT,
    	COALESCE([KST_GILTIG_FOM], '1899-12-31 00:00:00') AS KST_GILTIG_FOM,
    	COALESCE([KST_GILTIG_TOM], '1899-12-31 00:00:00') AS KST_GILTIG_TOM,
    	[KST_ID] AS KST_ID,
    	[KST_ID_TEXT] AS KST_ID_TEXT,
    	[KST_PASSIV] AS KST_PASSIV,
    	[KST_TEXT] AS KST_TEXT,
    	COALESCE([OMR_GILTIG_FOM], '1899-12-31 00:00:00') AS OMR_GILTIG_FOM,
    	COALESCE([OMR_GILTIG_TOM], '1899-12-31 00:00:00') AS OMR_GILTIG_TOM,
    	[OMR_ID] AS OMR_ID,
    	[OMR_ID_TEXT] AS OMR_ID_TEXT,
    	[OMR_PASSIV] AS OMR_PASSIV,
    	[OMR_TEXT] AS OMR_TEXT,
    	COALESCE([VERKS_GILTIG_FOM], '1899-12-31 00:00:00') AS VERKS_GILTIG_FOM,
    	COALESCE([VERKS_GILTIG_TOM], '1899-12-31 00:00:00') AS VERKS_GILTIG_TOM,
    	[VERKS_ID] AS VERKS_ID,
    	[VERKS_ID_TEXT] AS VERKS_ID_TEXT,
    	[VERKS_PASSIV] AS VERKS_PASSIV,
    	[VERKS_TEXT] AS VERKS_TEXT
    FROM [raindance_udp].[udp_100].[EK_DIM_OBJ_KST]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')