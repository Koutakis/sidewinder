from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_yg",
    source_entity="EK_DIM_OBJ_YG",
    table="ek_dim_obj_yg",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="YG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="YG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="YG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="YG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="YG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="YG_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([YG_GILTIG_FOM], '1899-12-31 00:00:00') AS YG_GILTIG_FOM,
    	COALESCE([YG_GILTIG_TOM], '1899-12-31 00:00:00') AS YG_GILTIG_TOM,
    	[YG_ID] AS YG_ID,
    	[YG_ID_TEXT] AS YG_ID_TEXT,
    	[YG_PASSIV] AS YG_PASSIV,
    	[YG_TEXT] AS YG_TEXT
    FROM [raindance_udp].[udp_150].[EK_DIM_OBJ_YG]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')