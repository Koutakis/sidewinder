from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_stökod_stökgr",
    source_entity="OBJSTRUKT_STÖKOD_STÖKGR",
    table="objstrukt_stökod_stökgr",
    schema="raindance_raw_1560",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_STÖKGR", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_STÖKOD", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['pvn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_STÖKGR] AS OBJ_STÖKGR,
    	[OBJ_STÖKOD] AS OBJ_STÖKOD
    FROM [utdata].[utdata156].[OBJSTRUKT_STÖKOD_STÖKGR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1560')