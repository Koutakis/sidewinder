from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_pskr_krptyp",
    source_entity="OBJSTRUKT_PSKR_KRPTYP",
    table="objstrukt_pskr_krptyp",
    schema="raindance_raw_8090",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_KRPTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_PSKR", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['medic', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_KRPTYP] AS OBJ_KRPTYP,
    	[OBJ_PSKR] AS OBJ_PSKR
    FROM [MediCarrierUDP].[utdata100].[OBJSTRUKT_PSKR_KRPTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8090')