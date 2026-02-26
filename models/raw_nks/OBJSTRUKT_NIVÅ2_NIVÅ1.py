from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_nivå2_nivå1",
    source_entity="OBJSTRUKT_NIVÅ2_NIVÅ1",
    table="objstrukt_nivå2_nivå1",
    schema="raindance_raw_2710",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_NIVÅ1", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_NIVÅ2", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_NIVÅ1] AS OBJ_NIVÅ1,
    	[OBJ_NIVÅ2] AS OBJ_NIVÅ2
    FROM [raindance_udp].[udp_100].[OBJSTRUKT_NIVÅ2_NIVÅ1]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')