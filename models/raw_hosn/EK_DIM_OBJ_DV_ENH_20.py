from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_dv_enh_20",
    source_entity="EK_DIM_OBJ_DV_ENH_20",
    table="ek_dim_obj_dv_enh_20",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="DV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="DV_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([DV_GILTIG_FOM], '1899-12-31 00:00:00') AS DV_GILTIG_FOM,
    	COALESCE([DV_GILTIG_TOM], '1899-12-31 00:00:00') AS DV_GILTIG_TOM,
    	[DV_ID] AS DV_ID,
    	[DV_ID_TEXT] AS DV_ID_TEXT,
    	[DV_PASSIV] AS DV_PASSIV,
    	[DV_TEXT] AS DV_TEXT
    FROM [utdata].[utdata150].[EK_DIM_OBJ_DV_ENH_20]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')