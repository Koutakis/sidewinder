from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ar_dim_obj_div",
    source_entity="AR_DIM_OBJ_DIV",
    table="ar_dim_obj_div",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DIV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DIV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DIV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="DIV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DIV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="DIV_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([DIV_GILTIG_FOM], '1899-12-31 00:00:00') AS DIV_GILTIG_FOM,
    	COALESCE([DIV_GILTIG_TOM], '1899-12-31 00:00:00') AS DIV_GILTIG_TOM,
    	[DIV_ID] AS DIV_ID,
    	[DIV_ID_TEXT] AS DIV_ID_TEXT,
    	[DIV_PASSIV] AS DIV_PASSIV,
    	[DIV_TEXT] AS DIV_TEXT
    FROM [Utdata].[udp_100].[AR_DIM_OBJ_DIV]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')