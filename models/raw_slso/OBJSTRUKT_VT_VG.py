from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_vt_vg",
    source_entity="OBJSTRUKT_VT_VG",
    table="objstrukt_vt_vg",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_VG", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_VT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_VG] AS OBJ_VG,
    	[OBJ_VT] AS OBJ_VT
    FROM [udpb4].[udpb4_100].[OBJSTRUKT_VT_VG]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1100')