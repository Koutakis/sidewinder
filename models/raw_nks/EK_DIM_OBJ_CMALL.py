from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_cmall",
    source_entity="EK_DIM_OBJ_CMALL",
    table="ek_dim_obj_cmall",
    schema="raindance_raw_nks",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="CMALL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="CMALL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="CMALL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="CMALL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="CMALL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="CMALL_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([CMALL_GILTIG_FOM], '1899-12-31 00:00:00') AS CMALL_GILTIG_FOM,
    	COALESCE([CMALL_GILTIG_TOM], '1899-12-31 00:00:00') AS CMALL_GILTIG_TOM,
    	[CMALL_ID] AS CMALL_ID,
    	[CMALL_ID_TEXT] AS CMALL_ID_TEXT,
    	[CMALL_PASSIV] AS CMALL_PASSIV,
    	[CMALL_TEXT] AS CMALL_TEXT
    FROM [raindance_udp].[udp_100].[EK_DIM_OBJ_CMALL]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')