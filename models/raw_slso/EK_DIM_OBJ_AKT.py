from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_akt",
    source_entity="EK_DIM_OBJ_AKT",
    table="ek_dim_obj_akt",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AKT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AKT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AKT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="AKT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AKT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="AKT_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([AKT_GILTIG_FOM], '1899-12-31 00:00:00') AS AKT_GILTIG_FOM,
    	COALESCE([AKT_GILTIG_TOM], '1899-12-31 00:00:00') AS AKT_GILTIG_TOM,
    	[AKT_ID] AS AKT_ID,
    	[AKT_ID_TEXT] AS AKT_ID_TEXT,
    	[AKT_PASSIV] AS AKT_PASSIV,
    	[AKT_TEXT] AS AKT_TEXT
    FROM [udpb4].[udpb4_100].[EK_DIM_OBJ_AKT]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1100')