from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_reskontra",
    source_entity="RK_DIM_RESKONTRA",
    table="rk_dim_reskontra",
    schema="raindance_raw_2990",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="EXTERN", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERN_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RESKONTRA", data_type=PostgresType.TEXT),
        PostgresColumn(name="RESKONTRA_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RESKTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="RESKTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['skade', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[EXTERN] AS EXTERN,
    	[EXTERN_TEXT] AS EXTERN_TEXT,
    	[RESKONTRA] AS RESKONTRA,
    	[RESKONTRA_TEXT] AS RESKONTRA_TEXT,
    	[RESKTYP] AS RESKTYP,
    	[RESKTYP_TEXT] AS RESKTYP_TEXT
    FROM [utdata].[utdata299].[RK_DIM_RESKONTRA]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2990')