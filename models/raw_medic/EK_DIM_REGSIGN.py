from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_regsign",
    source_entity="EK_DIM_REGSIGN",
    table="ek_dim_regsign",
    schema="raindance_raw_8090",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="REGSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="REGSIGN2", data_type=PostgresType.TEXT),
        PostgresColumn(name="REGSIGN2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="REGSIGN_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="REGSIGN_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['medic', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[REGSIGN] AS REGSIGN,
    	[REGSIGN2] AS REGSIGN2,
    	[REGSIGN2_ID_TEXT] AS REGSIGN2_ID_TEXT,
    	[REGSIGN_ID_TEXT] AS REGSIGN_ID_TEXT,
    	[REGSIGN_TEXT] AS REGSIGN_TEXT
    FROM [MediCarrierUDP].[utdata100].[EK_DIM_REGSIGN]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8090')