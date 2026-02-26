from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_plav",
    source_entity="EK_DIM_OBJ_PLAV",
    table="ek_dim_obj_plav",
    schema="raindance_raw_8570",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PLAV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PLAV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PLAV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PLAV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PLAV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PLAV_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sos', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PLAV_GILTIG_FOM], '1899-12-31 00:00:00') AS PLAV_GILTIG_FOM,
    	COALESCE([PLAV_GILTIG_TOM], '1899-12-31 00:00:00') AS PLAV_GILTIG_TOM,
    	[PLAV_ID] AS PLAV_ID,
    	[PLAV_ID_TEXT] AS PLAV_ID_TEXT,
    	[PLAV_PASSIV] AS PLAV_PASSIV,
    	[PLAV_TEXT] AS PLAV_TEXT
    FROM [raindance_udp].[udp_220].[EK_DIM_OBJ_PLAV]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8570')