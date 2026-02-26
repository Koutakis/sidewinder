from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_uppid",
    source_entity="EK_DIM_OBJ_UPPID",
    table="ek_dim_obj_uppid",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UPPID_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UPPID_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UPPID_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="UPPID_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="UPPID_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="UPPID_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([UPPID_GILTIG_FOM], '1899-12-31 00:00:00') AS UPPID_GILTIG_FOM,
    	COALESCE([UPPID_GILTIG_TOM], '1899-12-31 00:00:00') AS UPPID_GILTIG_TOM,
    	[UPPID_ID] AS UPPID_ID,
    	[UPPID_ID_TEXT] AS UPPID_ID_TEXT,
    	[UPPID_PASSIV] AS UPPID_PASSIV,
    	[UPPID_TEXT] AS UPPID_TEXT
    FROM [utdata].[utdata150].[EK_DIM_OBJ_UPPID]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')