from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_konto_hkrad",
    source_entity="OBJSTRUKT_KONTO_HKRAD",
    table="objstrukt_konto_hkrad",
    schema="raindance_raw_8810",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_HKRAD", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_KONTO", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ftsl', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_HKRAD] AS OBJ_HKRAD,
    	[OBJ_KONTO] AS OBJ_KONTO
    FROM [ftvudp].[ftv_400].[OBJSTRUKT_KONTO_HKRAD]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8810')