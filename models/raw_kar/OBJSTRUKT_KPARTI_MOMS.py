from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_kparti_moms",
    source_entity="OBJSTRUKT_KPARTI_MOMS",
    table="objstrukt_kparti_moms",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_KPARTI", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_MOMS", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_KPARTI] AS OBJ_KPARTI,
    	[OBJ_MOMS] AS OBJ_MOMS
    FROM [Utdata].[udp_100].[OBJSTRUKT_KPARTI_MOMS]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')