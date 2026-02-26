from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_geo",
    source_entity="EK_DIM_OBJ_GEO",
    table="ek_dim_obj_geo",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GEO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GEO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GEO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="GEO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="GEO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="GEO_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([GEO_GILTIG_FOM], '1899-12-31 00:00:00') AS GEO_GILTIG_FOM,
    	COALESCE([GEO_GILTIG_TOM], '1899-12-31 00:00:00') AS GEO_GILTIG_TOM,
    	[GEO_ID] AS GEO_ID,
    	[GEO_ID_TEXT] AS GEO_ID_TEXT,
    	[GEO_PASSIV] AS GEO_PASSIV,
    	[GEO_TEXT] AS GEO_TEXT
    FROM [utdata].[utdata150].[EK_DIM_OBJ_GEO]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')