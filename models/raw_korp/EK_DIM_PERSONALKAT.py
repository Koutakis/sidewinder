from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_personalkat",
    source_entity="EK_DIM_PERSONALKAT",
    table="ek_dim_personalkat",
    schema="raindance_raw_2870",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PERSONALKAT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PERSONALKAT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PERSONALKAT_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['korp', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[PERSONALKAT_ID] AS PERSONALKAT_ID,
    	[PERSONALKAT_ID_TEXT] AS PERSONALKAT_ID_TEXT,
    	[PERSONALKAT_TEXT] AS PERSONALKAT_TEXT
    FROM [utdata].[utdata287].[EK_DIM_PERSONALKAT]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2870')