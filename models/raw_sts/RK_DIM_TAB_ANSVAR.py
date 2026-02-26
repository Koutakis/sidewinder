from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_tab_ansvar",
    source_entity="RK_DIM_TAB_ANSVAR",
    table="rk_dim_tab_ansvar",
    schema="raindance_raw_8580",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_ANSVAR", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_ANSVAR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_ANSVAR_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sts', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DUMMY2] AS DUMMY2,
    	[TAB_ANSVAR] AS TAB_ANSVAR,
    	[TAB_ANSVAR_ID_TEXT] AS TAB_ANSVAR_ID_TEXT,
    	[TAB_ANSVAR_TEXT] AS TAB_ANSVAR_TEXT
    FROM [stsudp].[udp_858].[RK_DIM_TAB_ANSVAR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8580')