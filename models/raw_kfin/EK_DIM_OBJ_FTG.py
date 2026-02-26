from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_ftg",
    source_entity="EK_DIM_OBJ_FTG",
    table="ek_dim_obj_ftg",
    schema="raindance_raw_2930",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FTG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FTG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FTG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FTG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FTG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FTG_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kfin', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([FTG_GILTIG_FOM], '1899-12-31 00:00:00') AS FTG_GILTIG_FOM,
    	COALESCE([FTG_GILTIG_TOM], '1899-12-31 00:00:00') AS FTG_GILTIG_TOM,
    	[FTG_ID] AS FTG_ID,
    	[FTG_ID_TEXT] AS FTG_ID_TEXT,
    	[FTG_PASSIV] AS FTG_PASSIV,
    	[FTG_TEXT] AS FTG_TEXT
    FROM [utdata].[utdata293].[EK_DIM_OBJ_FTG]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2930')