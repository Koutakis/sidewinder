from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_avdel_rl",
    source_entity="OBJSTRUKT_AVDEL_RL",
    table="objstrukt_avdel_rl",
    schema="raindance_raw_2950",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_AVDEL", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_RL", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['rlk', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_AVDEL] AS OBJ_AVDEL,
    	[OBJ_RL] AS OBJ_RL
    FROM [utdata].[utdata295].[OBJSTRUKT_AVDEL_RL]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2950')