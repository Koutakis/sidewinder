from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ar_dim_status",
    source_entity="AR_DIM_STATUS",
    table="ar_dim_status",
    schema="raindance_raw_2930",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANLSTATUS", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANLSTATUS2", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANLSTATUS2_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANLSTATUS_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kfin', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[ANLSTATUS] AS ANLSTATUS,
    	[ANLSTATUS2] AS ANLSTATUS2,
    	[ANLSTATUS2_TEXT] AS ANLSTATUS2_TEXT,
    	[ANLSTATUS_TEXT] AS ANLSTATUS_TEXT
    FROM [utdata].[utdata293].[AR_DIM_STATUS]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2930')