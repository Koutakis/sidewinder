from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_tab_artist",
    source_entity="RK_DIM_TAB_ARTIST",
    table="rk_dim_tab_artist",
    schema="raindance_raw_3610",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DUMMY2", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_ARTIST", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_ARTIST_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TAB_ARTIST_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kultn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DUMMY2] AS DUMMY2,
    	[TAB_ARTIST] AS TAB_ARTIST,
    	[TAB_ARTIST_ID_TEXT] AS TAB_ARTIST_ID_TEXT,
    	[TAB_ARTIST_TEXT] AS TAB_ARTIST_TEXT
    FROM [utdata].[utdata361].[RK_DIM_TAB_ARTIST]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_3610')