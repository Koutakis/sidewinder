from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_detaljtyp",
    source_entity="RK_DIM_DETALJTYP",
    table="rk_dim_detaljtyp",
    schema="raindance_raw_2930",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DETALJTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="DETALJTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="DETALJTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DETALJTYP_NR", data_type=PostgresType.TEXT),
        PostgresColumn(name="DETALJTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kfin', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[DETALJTYP] AS DETALJTYP,
    	[DETALJTYP_ID] AS DETALJTYP_ID,
    	[DETALJTYP_ID_TEXT] AS DETALJTYP_ID_TEXT,
    	[DETALJTYP_NR] AS DETALJTYP_NR,
    	[DETALJTYP_TEXT] AS DETALJTYP_TEXT
    FROM [utdata].[utdata293].[RK_DIM_DETALJTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2930')