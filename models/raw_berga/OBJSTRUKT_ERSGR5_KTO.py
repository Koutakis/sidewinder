from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_ersgr5_kto",
    source_entity="OBJSTRUKT_ERSGR5_KTO",
    table="objstrukt_ersgr5_kto",
    schema="raindance_raw_2610",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ERSGR5", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_KTO", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['berga', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ERSGR5] AS OBJ_ERSGR5,
    	[OBJ_KTO] AS OBJ_KTO
    FROM [utdata].[utdata261].[OBJSTRUKT_ERSGR5_KTO]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2610')