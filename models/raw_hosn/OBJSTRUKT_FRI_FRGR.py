from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_fri_frgr",
    source_entity="OBJSTRUKT_FRI_FRGR",
    table="objstrukt_fri_frgr",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_FRGR", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_FRI", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_FRGR] AS OBJ_FRGR,
    	[OBJ_FRI] AS OBJ_FRI
    FROM [utdata].[utdata150].[OBJSTRUKT_FRI_FRGR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')