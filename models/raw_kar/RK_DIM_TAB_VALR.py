from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_tab_valr",
    source_entity="RK_DIM_TAB_VALR",
    table="rk_dim_tab_valr",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_VALR", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_VALR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_VALR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VARDE2", data_type=PostgresType.NUMERIC),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DUMMY2] AS DUMMY2,
    	[TAB_VALR] AS TAB_VALR,
    	[TAB_VALR_ID_TEXT] AS TAB_VALR_ID_TEXT,
    	[TAB_VALR_TEXT] AS TAB_VALR_TEXT,
    	[VARDE2] AS VARDE2
    FROM [Utdata].[udp_100].[RK_DIM_TAB_VALR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')