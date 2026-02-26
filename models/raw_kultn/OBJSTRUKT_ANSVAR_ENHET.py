from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_ansvar_enhet",
    source_entity="OBJSTRUKT_ANSVAR_ENHET",
    table="objstrukt_ansvar_enhet",
    schema="raindance_raw_3610",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ANSVAR", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_ENHET", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kultn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ANSVAR] AS OBJ_ANSVAR,
    	[OBJ_ENHET] AS OBJ_ENHET
    FROM [utdata].[utdata361].[OBJSTRUKT_ANSVAR_ENHET]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_3610')