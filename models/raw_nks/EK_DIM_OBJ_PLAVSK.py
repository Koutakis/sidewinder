from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_plavsk",
    source_entity="EK_DIM_OBJ_PLAVSK",
    table="ek_dim_obj_plavsk",
    schema="raindance_raw_nks",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PLAVSK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PLAVSK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PLAVSK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PLAVSK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PLAVSK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PLAVSK_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PLAVSK_GILTIG_FOM], '1899-12-31 00:00:00') AS PLAVSK_GILTIG_FOM,
    	COALESCE([PLAVSK_GILTIG_TOM], '1899-12-31 00:00:00') AS PLAVSK_GILTIG_TOM,
    	[PLAVSK_ID] AS PLAVSK_ID,
    	[PLAVSK_ID_TEXT] AS PLAVSK_ID_TEXT,
    	[PLAVSK_PASSIV] AS PLAVSK_PASSIV,
    	[PLAVSK_TEXT] AS PLAVSK_TEXT
    FROM [raindance_udp].[udp_100].[EK_DIM_OBJ_PLAVSK]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')