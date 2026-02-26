from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_defanl_fri2",
    source_entity="OBJSTRUKT_DEFANL_FRI2",
    table="objstrukt_defanl_fri2",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_DEFANL", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_FRI2", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_DEFANL] AS OBJ_DEFANL,
    	[OBJ_FRI2] AS OBJ_FRI2
    FROM [utdata].[utdata298].[OBJSTRUKT_DEFANL_FRI2]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2985')