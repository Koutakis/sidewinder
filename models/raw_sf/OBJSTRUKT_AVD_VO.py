from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_avd_vo",
    source_entity="OBJSTRUKT_AVD_VO",
    table="objstrukt_avd_vo",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_AVD", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_VO", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_AVD] AS OBJ_AVD,
    	[OBJ_VO] AS OBJ_VO
    FROM [utdata].[utdata298].[OBJSTRUKT_AVD_VO]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2985')