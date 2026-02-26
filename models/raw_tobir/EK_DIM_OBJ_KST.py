from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_kst",
    source_entity="EK_DIM_OBJ_KST",
    table="ek_dim_obj_kst",
    schema="raindance_raw_8050",
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
        PostgresColumn(name="ORG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ORG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ORG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ORG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ORG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ORG_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RESENH_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RESENH_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RESENH_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RESENH_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RESENH_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="RESENH_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VGREN_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VGREN_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VGREN_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['tobir', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([KST_GILTIG_FOM], '1899-12-31 00:00:00') AS KST_GILTIG_FOM,
    	COALESCE([KST_GILTIG_TOM], '1899-12-31 00:00:00') AS KST_GILTIG_TOM,
    	[KST_ID] AS KST_ID,
    	[KST_ID_TEXT] AS KST_ID_TEXT,
    	[KST_PASSIV] AS KST_PASSIV,
    	[KST_TEXT] AS KST_TEXT,
    	COALESCE([ORG_GILTIG_FOM], '1899-12-31 00:00:00') AS ORG_GILTIG_FOM,
    	COALESCE([ORG_GILTIG_TOM], '1899-12-31 00:00:00') AS ORG_GILTIG_TOM,
    	[ORG_ID] AS ORG_ID,
    	[ORG_ID_TEXT] AS ORG_ID_TEXT,
    	[ORG_PASSIV] AS ORG_PASSIV,
    	[ORG_TEXT] AS ORG_TEXT,
    	COALESCE([RESENH_GILTIG_FOM], '1899-12-31 00:00:00') AS RESENH_GILTIG_FOM,
    	COALESCE([RESENH_GILTIG_TOM], '1899-12-31 00:00:00') AS RESENH_GILTIG_TOM,
    	[RESENH_ID] AS RESENH_ID,
    	[RESENH_ID_TEXT] AS RESENH_ID_TEXT,
    	[RESENH_PASSIV] AS RESENH_PASSIV,
    	[RESENH_TEXT] AS RESENH_TEXT,
    	COALESCE([VGREN_GILTIG_FOM], '1899-12-31 00:00:00') AS VGREN_GILTIG_FOM,
    	COALESCE([VGREN_GILTIG_TOM], '1899-12-31 00:00:00') AS VGREN_GILTIG_TOM,
    	[VGREN_ID] AS VGREN_ID,
    	[VGREN_ID_TEXT] AS VGREN_ID_TEXT,
    	[VGREN_PASSIV] AS VGREN_PASSIV,
    	[VGREN_TEXT] AS VGREN_TEXT
    FROM [utdata].[utdata805].[EK_DIM_OBJ_KST]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8050')