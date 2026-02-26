from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_status",
    source_entity="RK_DIM_STATUS",
    table="rk_dim_status",
    schema="raindance_raw_2610",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAKTSTATUS", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTSTATUS_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTSTATUSTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTSTATUSTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['berga', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[FAKTSTATUS] AS FAKTSTATUS,
    	[FAKTSTATUS_TEXT] AS FAKTSTATUS_TEXT,
    	[FAKTSTATUSTYP] AS FAKTSTATUSTYP,
    	[FAKTSTATUSTYP_TEXT] AS FAKTSTATUSTYP_TEXT
    FROM [utdata].[utdata261].[RK_DIM_STATUS]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2610')