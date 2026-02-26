from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_kontsign",
    source_entity="EK_DIM_KONTSIGN",
    table="ek_dim_kontsign",
    schema="raindance_raw_2940",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KONTSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTSIGN2", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTSIGN2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTSIGN_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTSIGN_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sfit', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[KONTSIGN] AS KONTSIGN,
    	[KONTSIGN2] AS KONTSIGN2,
    	[KONTSIGN2_ID_TEXT] AS KONTSIGN2_ID_TEXT,
    	[KONTSIGN_ID_TEXT] AS KONTSIGN_ID_TEXT,
    	[KONTSIGN_TEXT] AS KONTSIGN_TEXT
    FROM [utdata].[utdata294].[EK_DIM_KONTSIGN]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2940')