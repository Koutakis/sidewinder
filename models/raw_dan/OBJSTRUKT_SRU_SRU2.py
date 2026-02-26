from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_sru_sru2",
    source_entity="OBJSTRUKT_SRU_SRU2",
    table="objstrukt_sru_sru2",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_SRU", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_SRU2", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_SRU] AS OBJ_SRU,
    	[OBJ_SRU2] AS OBJ_SRU2
    FROM [raindance_udp].[udp_150].[OBJSTRUKT_SRU_SRU2]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')