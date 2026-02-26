from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_attest",
    source_entity="RK_DIM_ATTEST",
    table="rk_dim_attest",
    schema="raindance_raw_2890",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTEST", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ATTEST_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['torpf', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[ATTEST] AS ATTEST,
    	[ATTEST_TEXT] AS ATTEST_TEXT
    FROM [utdata].[utdata289].[RK_DIM_ATTEST]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2890')