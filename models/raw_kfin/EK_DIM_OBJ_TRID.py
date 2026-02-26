from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_trid",
    source_entity="EK_DIM_OBJ_TRID",
    table="ek_dim_obj_trid",
    schema="raindance_raw_2930",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TRID_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TRID_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TRID_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TRID_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TRID_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="TRID_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kfin', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([TRID_GILTIG_FOM], '1899-12-31 00:00:00') AS TRID_GILTIG_FOM,
    	COALESCE([TRID_GILTIG_TOM], '1899-12-31 00:00:00') AS TRID_GILTIG_TOM,
    	[TRID_ID] AS TRID_ID,
    	[TRID_ID_TEXT] AS TRID_ID_TEXT,
    	[TRID_PASSIV] AS TRID_PASSIV,
    	[TRID_TEXT] AS TRID_TEXT
    FROM [utdata].[utdata293].[EK_DIM_OBJ_TRID]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2930')