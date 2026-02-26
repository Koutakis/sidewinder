from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_domän_babs",
    source_entity="OBJSTRUKT_DOMÄN_BABS",
    table="objstrukt_domän_babs",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_BABS", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_DOMÄN", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_BABS] AS OBJ_BABS,
    	[OBJ_DOMÄN] AS OBJ_DOMÄN
    FROM [utdata].[utdata150].[OBJSTRUKT_DOMÄN_BABS]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')