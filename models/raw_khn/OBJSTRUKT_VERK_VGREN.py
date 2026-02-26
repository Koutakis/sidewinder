from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_verk_vgren",
    source_entity="OBJSTRUKT_VERK_VGREN",
    table="objstrukt_verk_vgren",
    schema="raindance_raw_2880",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_VERK", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_VGREN", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['khn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_VERK] AS OBJ_VERK,
    	[OBJ_VGREN] AS OBJ_VGREN
    FROM [utdata].[utdata288].[OBJSTRUKT_VERK_VGREN]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2880')