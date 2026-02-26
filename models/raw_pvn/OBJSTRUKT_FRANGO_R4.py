from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_frango_r4",
    source_entity="OBJSTRUKT_FRANGO_R4",
    table="objstrukt_frango_r4",
    schema="raindance_raw_1560",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_FRANGO", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_R4", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['pvn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_FRANGO] AS OBJ_FRANGO,
    	[OBJ_R4] AS OBJ_R4
    FROM [utdata].[utdata156].[OBJSTRUKT_FRANGO_R4]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1560')