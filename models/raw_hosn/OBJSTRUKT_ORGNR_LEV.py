from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_orgnr_lev",
    source_entity="OBJSTRUKT_ORGNR_LEV",
    table="objstrukt_orgnr_lev",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_LEV", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_ORGNR", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_LEV] AS OBJ_LEV,
    	[OBJ_ORGNR] AS OBJ_ORGNR
    FROM [utdata].[utdata150].[OBJSTRUKT_ORGNR_LEV]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')