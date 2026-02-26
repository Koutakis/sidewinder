from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_mottattsign",
    source_entity="RK_DIM_MOTTATTSIGN",
    table="rk_dim_mottattsign",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTTATTSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTTATTSIGN2", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTTATTSIGN2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTTATTSIGN_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTTATTSIGN_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[MOTTATTSIGN] AS MOTTATTSIGN,
    	[MOTTATTSIGN2] AS MOTTATTSIGN2,
    	[MOTTATTSIGN2_ID_TEXT] AS MOTTATTSIGN2_ID_TEXT,
    	[MOTTATTSIGN_ID_TEXT] AS MOTTATTSIGN_ID_TEXT,
    	[MOTTATTSIGN_TEXT] AS MOTTATTSIGN_TEXT
    FROM [utdata].[utdata150].[RK_DIM_MOTTATTSIGN]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')