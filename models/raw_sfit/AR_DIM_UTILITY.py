from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ar_dim_utility",
    source_entity="AR_DIM_UTILITY",
    table="ar_dim_utility",
    schema="raindance_raw_2940",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UTILITY", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="UTILITY_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sfit', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[UTILITY] AS UTILITY,
    	[UTILITY_TEXT] AS UTILITY_TEXT
    FROM [utdata].[utdata294].[AR_DIM_UTILITY]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2940')