from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_attestsign2",
    source_entity="EK_DIM_ATTESTSIGN2",
    table="ek_dim_attestsign2",
    schema="raindance_raw_2990",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTESTSIGN2", data_type=PostgresType.TEXT),
        PostgresColumn(name="ATTESTSIGN22", data_type=PostgresType.TEXT),
        PostgresColumn(name="ATTESTSIGN22_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ATTESTSIGN2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ATTESTSIGN2_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['skade', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[ATTESTSIGN2] AS ATTESTSIGN2,
    	[ATTESTSIGN22] AS ATTESTSIGN22,
    	[ATTESTSIGN22_ID_TEXT] AS ATTESTSIGN22_ID_TEXT,
    	[ATTESTSIGN2_ID_TEXT] AS ATTESTSIGN2_ID_TEXT,
    	[ATTESTSIGN2_TEXT] AS ATTESTSIGN2_TEXT
    FROM [utdata].[utdata299].[EK_DIM_ATTESTSIGN2]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2990')