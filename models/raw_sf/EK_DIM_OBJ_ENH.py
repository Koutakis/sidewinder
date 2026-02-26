from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_enh",
    source_entity="EK_DIM_OBJ_ENH",
    table="ek_dim_obj_enh",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVDEL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVDEL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVDEL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVDEL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVDEL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="AVDEL_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENH_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ENH_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ENH_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ENH_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([AVDEL_GILTIG_FOM], '1899-12-31 00:00:00') AS AVDEL_GILTIG_FOM,
    	COALESCE([AVDEL_GILTIG_TOM], '1899-12-31 00:00:00') AS AVDEL_GILTIG_TOM,
    	[AVDEL_ID] AS AVDEL_ID,
    	[AVDEL_ID_TEXT] AS AVDEL_ID_TEXT,
    	[AVDEL_PASSIV] AS AVDEL_PASSIV,
    	[AVDEL_TEXT] AS AVDEL_TEXT,
    	COALESCE([ENH_GILTIG_FOM], '1899-12-31 00:00:00') AS ENH_GILTIG_FOM,
    	COALESCE([ENH_GILTIG_TOM], '1899-12-31 00:00:00') AS ENH_GILTIG_TOM,
    	[ENH_ID] AS ENH_ID,
    	[ENH_ID_TEXT] AS ENH_ID_TEXT,
    	[ENH_PASSIV] AS ENH_PASSIV,
    	[ENH_TEXT] AS ENH_TEXT
    FROM [utdata].[utdata298].[EK_DIM_OBJ_ENH]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2985')