from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_proj_xproja",
    source_entity="OBJSTRUKT_PROJ_XPROJA",
    table="objstrukt_proj_xproja",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_PROJ", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_XPROJA", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_PROJ] AS OBJ_PROJ,
    	[OBJ_XPROJA] AS OBJ_XPROJA
    FROM [raindance_udp].[udp_150].[OBJSTRUKT_PROJ_XPROJA]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')