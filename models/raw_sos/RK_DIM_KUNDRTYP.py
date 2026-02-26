from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="rk_dim_kundrtyp",
    source_entity="RK_DIM_KUNDRTYP",
    table="rk_dim_kundrtyp",
    schema="raindance_raw_8570",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KUNDRTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="KUNDRTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sos', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[KUNDRTYP] AS KUNDRTYP,
    	[KUNDRTYP_TEXT] AS KUNDRTYP_TEXT
    FROM [raindance_udp].[udp_220].[RK_DIM_KUNDRTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8570')