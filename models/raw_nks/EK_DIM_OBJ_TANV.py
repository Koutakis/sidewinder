from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_tanv",
    source_entity="EK_DIM_OBJ_TANV",
    table="ek_dim_obj_tanv",
    schema="raindance_raw_2710",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TANV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TANV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TANV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TANV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TANV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="TANV_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([TANV_GILTIG_FOM], '1899-12-31 00:00:00') AS TANV_GILTIG_FOM,
    	COALESCE([TANV_GILTIG_TOM], '1899-12-31 00:00:00') AS TANV_GILTIG_TOM,
    	[TANV_ID] AS TANV_ID,
    	[TANV_ID_TEXT] AS TANV_ID_TEXT,
    	[TANV_PASSIV] AS TANV_PASSIV,
    	[TANV_TEXT] AS TANV_TEXT
    FROM [raindance_udp].[udp_100].[EK_DIM_OBJ_TANV]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')