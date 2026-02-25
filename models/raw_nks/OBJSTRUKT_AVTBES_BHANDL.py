from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_avtbes_bhandl",
    source_entity="OBJSTRUKT_AVTBES_BHANDL",
    table="objstrukt_avtbes_bhandl",
    schema="raindance_raw_nks",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_AVTBES", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_BHANDL", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_AVTBES] AS OBJ_AVTBES,
    	[OBJ_BHANDL] AS OBJ_BHANDL
    FROM [raindance_udp].[udp_100].[OBJSTRUKT_AVTBES_BHANDL]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')