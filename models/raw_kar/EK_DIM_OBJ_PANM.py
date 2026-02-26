from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_panm",
    source_entity="EK_DIM_OBJ_PANM",
    table="ek_dim_obj_panm",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PANM_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PANM_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PANM_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PANM_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PANM_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PANM_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PANM_GILTIG_FOM], '1899-12-31 00:00:00') AS PANM_GILTIG_FOM,
    	COALESCE([PANM_GILTIG_TOM], '1899-12-31 00:00:00') AS PANM_GILTIG_TOM,
    	[PANM_ID] AS PANM_ID,
    	[PANM_ID_TEXT] AS PANM_ID_TEXT,
    	[PANM_PASSIV] AS PANM_PASSIV,
    	[PANM_TEXT] AS PANM_TEXT
    FROM [Utdata].[udp_100].[EK_DIM_OBJ_PANM]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')