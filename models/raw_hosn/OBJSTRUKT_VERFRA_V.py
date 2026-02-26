from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_verfra_v",
    source_entity="OBJSTRUKT_VERFRA_V",
    table="objstrukt_verfra_v",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_V", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_VERFRA", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_V] AS OBJ_V,
    	[OBJ_VERFRA] AS OBJ_VERFRA
    FROM [utdata].[utdata150].[OBJSTRUKT_VERFRA_V]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')