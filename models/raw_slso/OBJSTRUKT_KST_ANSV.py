from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_kst_ansv",
    source_entity="OBJSTRUKT_KST_ANSV",
    table="objstrukt_kst_ansv",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ANSV", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_KST", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ANSV] AS OBJ_ANSV,
    	[OBJ_KST] AS OBJ_KST
    FROM [udpb4].[udpb4_100].[OBJSTRUKT_KST_ANSV]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1100')