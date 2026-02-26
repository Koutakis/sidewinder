from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ar_dim_obj_kst",
    source_entity="AR_DIM_OBJ_KST",
    table="ar_dim_obj_kst",
    schema="raindance_raw_2610",
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
        PostgresColumn(name="VGREN_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VGREN_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VGREN_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VGREN_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VGREN_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['berga', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([ANSVAR_GILTIG_FOM], '1899-12-31 00:00:00') AS ANSVAR_GILTIG_FOM,
    	COALESCE([ANSVAR_GILTIG_TOM], '1899-12-31 00:00:00') AS ANSVAR_GILTIG_TOM,
    	[ANSVAR_ID] AS ANSVAR_ID,
    	[ANSVAR_ID_TEXT] AS ANSVAR_ID_TEXT,
    	[ANSVAR_PASSIV] AS ANSVAR_PASSIV,
    	[ANSVAR_TEXT] AS ANSVAR_TEXT,
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
    	COALESCE([VGREN_GILTIG_FOM], '1899-12-31 00:00:00') AS VGREN_GILTIG_FOM,
    	COALESCE([VGREN_GILTIG_TOM], '1899-12-31 00:00:00') AS VGREN_GILTIG_TOM,
    	[VGREN_ID] AS VGREN_ID,
    	[VGREN_ID_TEXT] AS VGREN_ID_TEXT,
    	[VGREN_PASSIV] AS VGREN_PASSIV,
    	[VGREN_TEXT] AS VGREN_TEXT
    FROM [utdata].[utdata261].[AR_DIM_OBJ_KST]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2610')