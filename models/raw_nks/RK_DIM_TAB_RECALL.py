from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_tab_recall",
    source_entity="RK_DIM_TAB_RECALL",
    table="rk_dim_tab_recall",
    schema="raindance_raw_nks",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_RECALL", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_RECALL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_RECALL_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DUMMY2] AS DUMMY2,
    	[TAB_RECALL] AS TAB_RECALL,
    	[TAB_RECALL_ID_TEXT] AS TAB_RECALL_ID_TEXT,
    	[TAB_RECALL_TEXT] AS TAB_RECALL_TEXT
    FROM [raindance_udp].[udp_100].[RK_DIM_TAB_RECALL]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')