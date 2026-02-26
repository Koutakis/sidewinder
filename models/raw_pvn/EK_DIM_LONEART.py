from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_loneart",
    source_entity="EK_DIM_LONEART",
    table="ek_dim_loneart",
    schema="raindance_raw_1560",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="LONEART_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="LONEART_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="LONEART_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="LONEART_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="LONEART_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="LONEART_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['pvn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([LONEART_GILTIG_FOM], '1899-12-31 00:00:00') AS LONEART_GILTIG_FOM,
    	COALESCE([LONEART_GILTIG_TOM], '1899-12-31 00:00:00') AS LONEART_GILTIG_TOM,
    	[LONEART_ID] AS LONEART_ID,
    	[LONEART_ID_TEXT] AS LONEART_ID_TEXT,
    	[LONEART_PASSIV] AS LONEART_PASSIV,
    	[LONEART_TEXT] AS LONEART_TEXT
    FROM [utdata].[utdata156].[EK_DIM_LONEART]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1560')