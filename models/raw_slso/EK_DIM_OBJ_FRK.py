from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_frk",
    source_entity="EK_DIM_OBJ_FRK",
    table="ek_dim_obj_frk",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRG_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRK_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([FRG_GILTIG_FOM], '1899-12-31 00:00:00') AS FRG_GILTIG_FOM,
    	COALESCE([FRG_GILTIG_TOM], '1899-12-31 00:00:00') AS FRG_GILTIG_TOM,
    	[FRG_ID] AS FRG_ID,
    	[FRG_ID_TEXT] AS FRG_ID_TEXT,
    	[FRG_PASSIV] AS FRG_PASSIV,
    	[FRG_TEXT] AS FRG_TEXT,
    	COALESCE([FRK_GILTIG_FOM], '1899-12-31 00:00:00') AS FRK_GILTIG_FOM,
    	COALESCE([FRK_GILTIG_TOM], '1899-12-31 00:00:00') AS FRK_GILTIG_TOM,
    	[FRK_ID] AS FRK_ID,
    	[FRK_ID_TEXT] AS FRK_ID_TEXT,
    	[FRK_PASSIV] AS FRK_PASSIV,
    	[FRK_TEXT] AS FRK_TEXT
    FROM [udpb4].[udpb4_100].[EK_DIM_OBJ_FRK]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1100')