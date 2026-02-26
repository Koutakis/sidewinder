from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_tab_momsbe",
    source_entity="RK_DIM_TAB_MOMSBE",
    table="rk_dim_tab_momsbe",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_MOMSBE", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_MOMSBE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_MOMSBE_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DUMMY2] AS DUMMY2,
    	[TAB_MOMSBE] AS TAB_MOMSBE,
    	[TAB_MOMSBE_ID_TEXT] AS TAB_MOMSBE_ID_TEXT,
    	[TAB_MOMSBE_TEXT] AS TAB_MOMSBE_TEXT
    FROM [raindance_udp].[udp_150].[RK_DIM_TAB_MOMSBE]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')