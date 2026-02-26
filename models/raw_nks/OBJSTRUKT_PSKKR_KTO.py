from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_pskkr_kto",
    source_entity="OBJSTRUKT_PSKKR_KTO",
    table="objstrukt_pskkr_kto",
    schema="raindance_raw_2710",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_KTO", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_PSKKR", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_KTO] AS OBJ_KTO,
    	[OBJ_PSKKR] AS OBJ_PSKKR
    FROM [raindance_udp].[udp_100].[OBJSTRUKT_PSKKR_KTO]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')