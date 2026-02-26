from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_kto_fr01",
    source_entity="OBJSTRUKT_KTO_FR01",
    table="objstrukt_kto_fr01",
    schema="raindance_raw_8090",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_FR01", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_KTO", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['medic', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_FR01] AS OBJ_FR01,
    	[OBJ_KTO] AS OBJ_KTO
    FROM [MediCarrierUDP].[utdata100].[OBJSTRUKT_KTO_FR01]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8090')