from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_tab_prel",
    source_entity="RK_DIM_TAB_PREL",
    table="rk_dim_tab_prel",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_PREL", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_PREL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_PREL_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DUMMY2] AS DUMMY2,
    	[TAB_PREL] AS TAB_PREL,
    	[TAB_PREL_ID_TEXT] AS TAB_PREL_ID_TEXT,
    	[TAB_PREL_TEXT] AS TAB_PREL_TEXT
    FROM [udpb4].[udpb4_100].[RK_DIM_TAB_PREL]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1100')