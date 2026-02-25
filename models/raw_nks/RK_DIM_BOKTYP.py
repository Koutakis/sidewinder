from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_boktyp",
    source_entity="RK_DIM_BOKTYP",
    table="rk_dim_boktyp",
    schema="raindance_raw_nks",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BOKTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOKTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOKTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOKTYP_NR", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOKTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[BOKTYP] AS BOKTYP,
    	[BOKTYP_ID] AS BOKTYP_ID,
    	[BOKTYP_ID_TEXT] AS BOKTYP_ID_TEXT,
    	[BOKTYP_NR] AS BOKTYP_NR,
    	[BOKTYP_TEXT] AS BOKTYP_TEXT
    FROM [raindance_udp].[udp_100].[RK_DIM_BOKTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')