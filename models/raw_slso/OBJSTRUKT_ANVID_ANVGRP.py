from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_anvid_anvgrp",
    source_entity="OBJSTRUKT_ANVID_ANVGRP",
    table="objstrukt_anvid_anvgrp",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_ANVGRP", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_ANVID", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_ANVGRP] AS OBJ_ANVGRP,
    	[OBJ_ANVID] AS OBJ_ANVID
    FROM [udpb4].[udpb4_100].[OBJSTRUKT_ANVID_ANVGRP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1100')