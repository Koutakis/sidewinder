from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_tab_ubk",
    source_entity="RK_DIM_TAB_UBK",
    table="rk_dim_tab_ubk",
    schema="raindance_raw_2880",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_UBK", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_UBK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_UBK_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VARDE1", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VARDE2", data_type=PostgresType.NUMERIC),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['khn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DUMMY2] AS DUMMY2,
    	[TAB_UBK] AS TAB_UBK,
    	[TAB_UBK_ID_TEXT] AS TAB_UBK_ID_TEXT,
    	[TAB_UBK_TEXT] AS TAB_UBK_TEXT,
    	[VARDE1] AS VARDE1,
    	[VARDE2] AS VARDE2
    FROM [utdata].[utdata288].[RK_DIM_TAB_UBK]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2880')