from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_anstform",
    source_entity="EK_DIM_ANSTFORM",
    table="ek_dim_anstform",
    schema="raindance_raw_8410",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSTFORM_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSTFORM_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSTFORM_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['lis', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[ANSTFORM_ID] AS ANSTFORM_ID,
    	[ANSTFORM_ID_TEXT] AS ANSTFORM_ID_TEXT,
    	[ANSTFORM_TEXT] AS ANSTFORM_TEXT
    FROM [utdata].[utdata840].[EK_DIM_ANSTFORM]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8410')