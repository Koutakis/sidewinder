from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_kst_ans2",
    source_entity="OBJSTRUKT_KST_ANS2",
    table="objstrukt_kst_ans2",
    schema="raindance_raw_2920",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ANS2", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_KST", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['rk', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ANS2] AS OBJ_ANS2,
    	[OBJ_KST] AS OBJ_KST
    FROM [utdata].[utdata292].[OBJSTRUKT_KST_ANS2]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2920')