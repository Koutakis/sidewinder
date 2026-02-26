from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_anstsign",
    source_entity="RK_DIM_ANSTSIGN",
    table="rk_dim_anstsign",
    schema="raindance_raw_8810",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSTSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSTSIGN2", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSTSIGN2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSTSIGN_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSTSIGN_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ftsl', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[ANSTSIGN] AS ANSTSIGN,
    	[ANSTSIGN2] AS ANSTSIGN2,
    	[ANSTSIGN2_ID_TEXT] AS ANSTSIGN2_ID_TEXT,
    	[ANSTSIGN_ID_TEXT] AS ANSTSIGN_ID_TEXT,
    	[ANSTSIGN_TEXT] AS ANSTSIGN_TEXT
    FROM [ftvudp].[ftv_400].[RK_DIM_ANSTSIGN]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8810')