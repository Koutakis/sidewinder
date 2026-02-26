from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_enhet_ftg",
    source_entity="OBJSTRUKT_ENHET_FTG",
    table="objstrukt_enhet_ftg",
    schema="raindance_raw_8010",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ENHET", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_FTG", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['films', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ENHET] AS OBJ_ENHET,
    	[OBJ_FTG] AS OBJ_FTG
    FROM [utdata].[utdata801].[OBJSTRUKT_ENHET_FTG]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8010')