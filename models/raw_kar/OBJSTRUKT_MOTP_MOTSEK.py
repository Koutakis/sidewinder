from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="objstrukt_motp_motsek",
    source_entity="OBJSTRUKT_MOTP_MOTSEK",
    table="objstrukt_motp_motsek",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OBJ_MOTP", data_type=PostgresType.TEXT),
        PostgresColumn(name="OBJ_MOTSEK", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	[OBJ_MOTP] AS OBJ_MOTP,
    	[OBJ_MOTSEK] AS OBJ_MOTSEK
    FROM [Utdata].[udp_100].[OBJSTRUKT_MOTP_MOTSEK]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')