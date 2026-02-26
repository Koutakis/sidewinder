from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_övtyp_urspr",
    source_entity="OBJSTRUKT_ÖVTYP_URSPR",
    table="objstrukt_övtyp_urspr",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_URSPR", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_ÖVTYP", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_URSPR] AS OBJ_URSPR,
    	[OBJ_ÖVTYP] AS OBJ_ÖVTYP
    FROM [Utdata].[udp_100].[OBJSTRUKT_ÖVTYP_URSPR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')