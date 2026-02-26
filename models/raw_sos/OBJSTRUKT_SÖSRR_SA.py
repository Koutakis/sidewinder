from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_sösrr_sa",
    source_entity="OBJSTRUKT_SÖSRR_SA",
    table="objstrukt_sösrr_sa",
    schema="raindance_raw_8570",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_SA", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_SÖSRR", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sos', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_SA] AS OBJ_SA,
    	[OBJ_SÖSRR] AS OBJ_SÖSRR
    FROM [raindance_udp].[udp_220].[OBJSTRUKT_SÖSRR_SA]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8570')