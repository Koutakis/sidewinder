from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_kassa",
    source_entity="EK_DIM_OBJ_KASSA",
    table="ek_dim_obj_kassa",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KASSA_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KASSA_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KASSA_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KASSA_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KASSA_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KASSA_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([KASSA_GILTIG_FOM], '1899-12-31 00:00:00') AS KASSA_GILTIG_FOM,
    	COALESCE([KASSA_GILTIG_TOM], '1899-12-31 00:00:00') AS KASSA_GILTIG_TOM,
    	[KASSA_ID] AS KASSA_ID,
    	[KASSA_ID_TEXT] AS KASSA_ID_TEXT,
    	[KASSA_PASSIV] AS KASSA_PASSIV,
    	[KASSA_TEXT] AS KASSA_TEXT
    FROM [raindance_udp].[udp_150].[EK_DIM_OBJ_KASSA]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')