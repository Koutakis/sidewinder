from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_prod",
    source_entity="EK_DIM_OBJ_PROD",
    table="ek_dim_obj_prod",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROD_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROD_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROD_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROD_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROD_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PROD_GILTIG_FOM], '1899-12-31 00:00:00') AS PROD_GILTIG_FOM,
    	COALESCE([PROD_GILTIG_TOM], '1899-12-31 00:00:00') AS PROD_GILTIG_TOM,
    	[PROD_ID] AS PROD_ID,
    	[PROD_ID_TEXT] AS PROD_ID_TEXT,
    	[PROD_PASSIV] AS PROD_PASSIV,
    	[PROD_TEXT] AS PROD_TEXT
    FROM [Utdata].[udp_100].[EK_DIM_OBJ_PROD]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')