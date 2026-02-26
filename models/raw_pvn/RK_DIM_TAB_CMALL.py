from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_tab_cmall",
    source_entity="RK_DIM_TAB_CMALL",
    table="rk_dim_tab_cmall",
    schema="raindance_raw_1560",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_CMALL", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_CMALL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_CMALL_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['pvn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DUMMY2] AS DUMMY2,
    	[TAB_CMALL] AS TAB_CMALL,
    	[TAB_CMALL_ID_TEXT] AS TAB_CMALL_ID_TEXT,
    	[TAB_CMALL_TEXT] AS TAB_CMALL_TEXT
    FROM [utdata].[utdata156].[RK_DIM_TAB_CMALL]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1560')