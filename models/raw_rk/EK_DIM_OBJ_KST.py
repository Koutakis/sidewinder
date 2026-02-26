from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_kst",
    source_entity="EK_DIM_OBJ_KST",
    table="ek_dim_obj_kst",
    schema="raindance_raw_2920",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANS1_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANS1_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANS1_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANS1_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANS1_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANS1_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANS2_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANS2_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANS2_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANS2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANS2_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANS2_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KST_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VO_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['rk', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([ANS1_GILTIG_FOM], '1899-12-31 00:00:00') AS ANS1_GILTIG_FOM,
    	COALESCE([ANS1_GILTIG_TOM], '1899-12-31 00:00:00') AS ANS1_GILTIG_TOM,
    	[ANS1_ID] AS ANS1_ID,
    	[ANS1_ID_TEXT] AS ANS1_ID_TEXT,
    	[ANS1_PASSIV] AS ANS1_PASSIV,
    	[ANS1_TEXT] AS ANS1_TEXT,
    	COALESCE([ANS2_GILTIG_FOM], '1899-12-31 00:00:00') AS ANS2_GILTIG_FOM,
    	COALESCE([ANS2_GILTIG_TOM], '1899-12-31 00:00:00') AS ANS2_GILTIG_TOM,
    	[ANS2_ID] AS ANS2_ID,
    	[ANS2_ID_TEXT] AS ANS2_ID_TEXT,
    	[ANS2_PASSIV] AS ANS2_PASSIV,
    	[ANS2_TEXT] AS ANS2_TEXT,
    	COALESCE([KST_GILTIG_FOM], '1899-12-31 00:00:00') AS KST_GILTIG_FOM,
    	COALESCE([KST_GILTIG_TOM], '1899-12-31 00:00:00') AS KST_GILTIG_TOM,
    	[KST_ID] AS KST_ID,
    	[KST_ID_TEXT] AS KST_ID_TEXT,
    	[KST_PASSIV] AS KST_PASSIV,
    	[KST_TEXT] AS KST_TEXT,
    	COALESCE([VO_GILTIG_FOM], '1899-12-31 00:00:00') AS VO_GILTIG_FOM,
    	COALESCE([VO_GILTIG_TOM], '1899-12-31 00:00:00') AS VO_GILTIG_TOM,
    	[VO_ID] AS VO_ID,
    	[VO_ID_TEXT] AS VO_ID_TEXT,
    	[VO_PASSIV] AS VO_PASSIV,
    	[VO_TEXT] AS VO_TEXT
    FROM [utdata].[utdata292].[EK_DIM_OBJ_KST]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2920')