from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_anlobj_anskv",
    source_entity="OBJSTRUKT_ANLOBJ_ANSKV",
    table="objstrukt_anlobj_anskv",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ANLOBJ", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_ANSKV", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ANLOBJ] AS OBJ_ANLOBJ,
    	[OBJ_ANSKV] AS OBJ_ANSKV
    FROM [Utdata].[udp_100].[OBJSTRUKT_ANLOBJ_ANSKV]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')