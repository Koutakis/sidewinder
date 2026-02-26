from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_abobg_abolev",
    source_entity="OBJSTRUKT_ABOBG_ABOLEV",
    table="objstrukt_abobg_abolev",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ABOBG", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_ABOLEV", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ABOBG] AS OBJ_ABOBG,
    	[OBJ_ABOLEV] AS OBJ_ABOLEV
    FROM [utdata].[utdata150].[OBJSTRUKT_ABOBG_ABOLEV]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')