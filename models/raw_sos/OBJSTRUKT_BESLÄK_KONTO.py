from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_besläk_konto",
    source_entity="OBJSTRUKT_BESLÄK_KONTO",
    table="objstrukt_besläk_konto",
    schema="raindance_raw_8570",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_BESLÄK", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_KONTO", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sos', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_BESLÄK] AS OBJ_BESLÄK,
    	[OBJ_KONTO] AS OBJ_KONTO
    FROM [raindance_udp].[udp_220].[OBJSTRUKT_BESLÄK_KONTO]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8570')