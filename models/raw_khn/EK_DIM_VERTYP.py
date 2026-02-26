from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_vertyp",
    source_entity="EK_DIM_VERTYP",
    table="ek_dim_vertyp",
    schema="raindance_raw_2880",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DELSYSTEM", data_type=PostgresType.TEXT),
        PostgresColumn(name="DELSYSTEM_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERTYP_PASSIV", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['khn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DELSYSTEM] AS DELSYSTEM,
    	[DELSYSTEM_TEXT] AS DELSYSTEM_TEXT,
    	[VERTYP] AS VERTYP,
    	[VERTYP_PASSIV] AS VERTYP_PASSIV,
    	[VERTYP_TEXT] AS VERTYP_TEXT
    FROM [utdata].[utdata288].[EK_DIM_VERTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2880')