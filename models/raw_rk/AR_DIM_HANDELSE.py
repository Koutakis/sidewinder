from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ar_dim_handelse",
    source_entity="AR_DIM_HANDELSE",
    table="ar_dim_handelse",
    schema="raindance_raw_2920",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="HANDELSE", data_type=PostgresType.TEXT),
        PostgresColumn(name="HANDELSE2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="HANDELSE2_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="HANDELSE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="HANDELSE_ORDNING", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="HANDELSE_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['rk', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[HANDELSE] AS HANDELSE,
    	[HANDELSE2_ID_TEXT] AS HANDELSE2_ID_TEXT,
    	[HANDELSE2_TEXT] AS HANDELSE2_TEXT,
    	[HANDELSE_ID_TEXT] AS HANDELSE_ID_TEXT,
    	[HANDELSE_ORDNING] AS HANDELSE_ORDNING,
    	[HANDELSE_TEXT] AS HANDELSE_TEXT
    FROM [utdata].[utdata292].[AR_DIM_HANDELSE]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2920')