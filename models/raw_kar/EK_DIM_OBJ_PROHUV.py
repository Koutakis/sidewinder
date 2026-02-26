from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_prohuv",
    source_entity="EK_DIM_OBJ_PROHUV",
    table="ek_dim_obj_prohuv",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROHUV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROHUV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROHUV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROHUV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROHUV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROHUV_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PROHUV_GILTIG_FOM], '1899-12-31 00:00:00') AS PROHUV_GILTIG_FOM,
    	COALESCE([PROHUV_GILTIG_TOM], '1899-12-31 00:00:00') AS PROHUV_GILTIG_TOM,
    	[PROHUV_ID] AS PROHUV_ID,
    	[PROHUV_ID_TEXT] AS PROHUV_ID_TEXT,
    	[PROHUV_PASSIV] AS PROHUV_PASSIV,
    	[PROHUV_TEXT] AS PROHUV_TEXT
    FROM [Utdata].[udp_100].[EK_DIM_OBJ_PROHUV]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')