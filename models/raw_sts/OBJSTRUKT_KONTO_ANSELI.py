from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_konto_anseli",
    source_entity="OBJSTRUKT_KONTO_ANSELI",
    table="objstrukt_konto_anseli",
    schema="raindance_raw_8580",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ANSELI", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_KONTO", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sts', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ANSELI] AS OBJ_ANSELI,
    	[OBJ_KONTO] AS OBJ_KONTO
    FROM [stsudp].[udp_858].[OBJSTRUKT_KONTO_ANSELI]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8580')