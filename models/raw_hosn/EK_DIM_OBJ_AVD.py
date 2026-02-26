from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_avd",
    source_entity="EK_DIM_OBJ_AVD",
    table="ek_dim_obj_avd",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVD_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVD_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="AVD_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RAM_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RAM_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RAM_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RAM_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RAM_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="RAM_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([AVD_GILTIG_FOM], '1899-12-31 00:00:00') AS AVD_GILTIG_FOM,
    	COALESCE([AVD_GILTIG_TOM], '1899-12-31 00:00:00') AS AVD_GILTIG_TOM,
    	[AVD_ID] AS AVD_ID,
    	[AVD_ID_TEXT] AS AVD_ID_TEXT,
    	[AVD_PASSIV] AS AVD_PASSIV,
    	[AVD_TEXT] AS AVD_TEXT,
    	COALESCE([RAM_GILTIG_FOM], '1899-12-31 00:00:00') AS RAM_GILTIG_FOM,
    	COALESCE([RAM_GILTIG_TOM], '1899-12-31 00:00:00') AS RAM_GILTIG_TOM,
    	[RAM_ID] AS RAM_ID,
    	[RAM_ID_TEXT] AS RAM_ID_TEXT,
    	[RAM_PASSIV] AS RAM_PASSIV,
    	[RAM_TEXT] AS RAM_TEXT
    FROM [utdata].[utdata150].[EK_DIM_OBJ_AVD]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')