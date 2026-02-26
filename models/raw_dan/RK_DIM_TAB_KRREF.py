from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_tab_krref",
    source_entity="RK_DIM_TAB_KRREF",
    table="rk_dim_tab_krref",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_KRREF", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_KRREF_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_KRREF_TEXT", data_type=PostgresType.TEXT),
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
    	[TAB_KRREF] AS TAB_KRREF,
    	[TAB_KRREF_ID_TEXT] AS TAB_KRREF_ID_TEXT,
    	[TAB_KRREF_TEXT] AS TAB_KRREF_TEXT
    FROM [raindance_udp].[udp_150].[RK_DIM_TAB_KRREF]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')