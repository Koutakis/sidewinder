from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ar_dim_beloppstyp",
    source_entity="AR_DIM_BELOPPSTYP",
    table="ar_dim_beloppstyp",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BELOPPSTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="BELOPPSTYP2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BELOPPSTYP2_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BELOPPSTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BELOPPSTYP_ORDNING", data_type=PostgresType.TEXT),
        PostgresColumn(name="BELOPPSTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[BELOPPSTYP] AS BELOPPSTYP,
    	[BELOPPSTYP2_ID_TEXT] AS BELOPPSTYP2_ID_TEXT,
    	[BELOPPSTYP2_TEXT] AS BELOPPSTYP2_TEXT,
    	[BELOPPSTYP_ID_TEXT] AS BELOPPSTYP_ID_TEXT,
    	[BELOPPSTYP_ORDNING] AS BELOPPSTYP_ORDNING,
    	[BELOPPSTYP_TEXT] AS BELOPPSTYP_TEXT
    FROM [Utdata].[udp_100].[AR_DIM_BELOPPSTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')