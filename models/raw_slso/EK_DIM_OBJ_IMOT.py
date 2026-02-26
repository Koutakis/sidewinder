from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_imot",
    source_entity="EK_DIM_OBJ_IMOT",
    table="ek_dim_obj_imot",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="IMOT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="IMOT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="IMOT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="IMOT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="IMOT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="IMOT_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="REIMOT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="REIMOT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="REIMOT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="REIMOT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="REIMOT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="REIMOT_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([IMOT_GILTIG_FOM], '1899-12-31 00:00:00') AS IMOT_GILTIG_FOM,
    	COALESCE([IMOT_GILTIG_TOM], '1899-12-31 00:00:00') AS IMOT_GILTIG_TOM,
    	[IMOT_ID] AS IMOT_ID,
    	[IMOT_ID_TEXT] AS IMOT_ID_TEXT,
    	[IMOT_PASSIV] AS IMOT_PASSIV,
    	[IMOT_TEXT] AS IMOT_TEXT,
    	COALESCE([REIMOT_GILTIG_FOM], '1899-12-31 00:00:00') AS REIMOT_GILTIG_FOM,
    	COALESCE([REIMOT_GILTIG_TOM], '1899-12-31 00:00:00') AS REIMOT_GILTIG_TOM,
    	[REIMOT_ID] AS REIMOT_ID,
    	[REIMOT_ID_TEXT] AS REIMOT_ID_TEXT,
    	[REIMOT_PASSIV] AS REIMOT_PASSIV,
    	[REIMOT_TEXT] AS REIMOT_TEXT
    FROM [udpb4].[udpb4_100].[EK_DIM_OBJ_IMOT]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1100')