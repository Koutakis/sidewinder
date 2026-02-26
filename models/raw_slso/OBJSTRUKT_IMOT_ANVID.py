from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_imot_anvid",
    source_entity="OBJSTRUKT_IMOT_ANVID",
    table="objstrukt_imot_anvid",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ANVID", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_IMOT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ANVID] AS OBJ_ANVID,
    	[OBJ_IMOT] AS OBJ_IMOT
    FROM [udpb4].[udpb4_100].[OBJSTRUKT_IMOT_ANVID]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1100')