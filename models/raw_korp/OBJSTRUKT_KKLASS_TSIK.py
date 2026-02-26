from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_kklass_tsik",
    source_entity="OBJSTRUKT_KKLASS_TSIK",
    table="objstrukt_kklass_tsik",
    schema="raindance_raw_2870",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_KKLASS", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_TSIK", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['korp', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_KKLASS] AS OBJ_KKLASS,
    	[OBJ_TSIK] AS OBJ_TSIK
    FROM [utdata].[utdata287].[OBJSTRUKT_KKLASS_TSIK]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2870')