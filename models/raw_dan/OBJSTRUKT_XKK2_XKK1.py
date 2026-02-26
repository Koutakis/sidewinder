from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_xkk2_xkk1",
    source_entity="OBJSTRUKT_XKK2_XKK1",
    table="objstrukt_xkk2_xkk1",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_XKK1", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_XKK2", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_XKK1] AS OBJ_XKK1,
    	[OBJ_XKK2] AS OBJ_XKK2
    FROM [raindance_udp].[udp_150].[OBJSTRUKT_XKK2_XKK1]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')