from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_ib",
    source_entity="EK_DIM_IB",
    table="ek_dim_ib",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="IB", data_type=PostgresType.TEXT),
        PostgresColumn(name="IB_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[IB] AS IB,
    	[IB_TEXT] AS IB_TEXT
    FROM [utdata].[utdata298].[EK_DIM_IB]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2985')