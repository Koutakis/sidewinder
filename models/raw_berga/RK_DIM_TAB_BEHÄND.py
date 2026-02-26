from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_tab_behänd",
    source_entity="RK_DIM_TAB_BEHÄND",
    table="rk_dim_tab_behänd",
    schema="raindance_raw_2610",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_BEHÄND", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_BEHÄND_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_BEHÄND_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['berga', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DUMMY2] AS DUMMY2,
    	[TAB_BEHÄND] AS TAB_BEHÄND,
    	[TAB_BEHÄND_ID_TEXT] AS TAB_BEHÄND_ID_TEXT,
    	[TAB_BEHÄND_TEXT] AS TAB_BEHÄND_TEXT
    FROM [utdata].[utdata261].[RK_DIM_TAB_BEHÄND]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2610')