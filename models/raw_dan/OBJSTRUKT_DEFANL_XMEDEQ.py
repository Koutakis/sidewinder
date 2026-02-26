from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_defanl_xmedeq",
    source_entity="OBJSTRUKT_DEFANL_XMEDEQ",
    table="objstrukt_defanl_xmedeq",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_DEFANL", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_XMEDEQ", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_DEFANL] AS OBJ_DEFANL,
    	[OBJ_XMEDEQ] AS OBJ_XMEDEQ
    FROM [raindance_udp].[udp_150].[OBJSTRUKT_DEFANL_XMEDEQ]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')