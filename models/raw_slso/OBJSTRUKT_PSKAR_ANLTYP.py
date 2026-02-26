from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_pskar_anltyp",
    source_entity="OBJSTRUKT_PSKAR_ANLTYP",
    table="objstrukt_pskar_anltyp",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ANLTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_PSKAR", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ANLTYP] AS OBJ_ANLTYP,
    	[OBJ_PSKAR] AS OBJ_PSKAR
    FROM [udpb4].[udpb4_100].[OBJSTRUKT_PSKAR_ANLTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1100')