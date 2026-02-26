from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_vernr",
    source_entity="EK_DIM_VERNR",
    table="ek_dim_vernr",
    schema="raindance_raw_2610",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AR", data_type=PostgresType.TEXT),
        PostgresColumn(name="INTERNVERNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="INTERNVERNR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERNRGRUPP", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['berga', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[AR] AS AR,
    	[INTERNVERNR] AS INTERNVERNR,
    	[INTERNVERNR_TEXT] AS INTERNVERNR_TEXT,
    	COALESCE([VERDATUM], '1899-12-31 00:00:00') AS VERDATUM,
    	[VERNRGRUPP] AS VERNRGRUPP
    FROM [utdata].[utdata261].[EK_DIM_VERNR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2610')