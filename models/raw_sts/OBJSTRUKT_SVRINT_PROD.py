from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_svrint_prod",
    source_entity="OBJSTRUKT_SVRINT_PROD",
    table="objstrukt_svrint_prod",
    schema="raindance_raw_8580",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_PROD", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_SVRINT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sts', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_PROD] AS OBJ_PROD,
    	[OBJ_SVRINT] AS OBJ_SVRINT
    FROM [stsudp].[udp_858].[OBJSTRUKT_SVRINT_PROD]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8580')