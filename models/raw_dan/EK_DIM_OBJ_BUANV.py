from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_buanv",
    source_entity="EK_DIM_OBJ_BUANV",
    table="ek_dim_obj_buanv",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BUANV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BUANV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BUANV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="BUANV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BUANV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="BUANV_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([BUANV_GILTIG_FOM], '1899-12-31 00:00:00') AS BUANV_GILTIG_FOM,
    	COALESCE([BUANV_GILTIG_TOM], '1899-12-31 00:00:00') AS BUANV_GILTIG_TOM,
    	[BUANV_ID] AS BUANV_ID,
    	[BUANV_ID_TEXT] AS BUANV_ID_TEXT,
    	[BUANV_PASSIV] AS BUANV_PASSIV,
    	[BUANV_TEXT] AS BUANV_TEXT
    FROM [raindance_udp].[udp_150].[EK_DIM_OBJ_BUANV]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')