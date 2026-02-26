from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_proc",
    source_entity="EK_DIM_OBJ_PROC",
    table="ek_dim_obj_proc",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROC_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROC_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROC_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROC_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROC_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROC_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PROC_GILTIG_FOM], '1899-12-31 00:00:00') AS PROC_GILTIG_FOM,
    	COALESCE([PROC_GILTIG_TOM], '1899-12-31 00:00:00') AS PROC_GILTIG_TOM,
    	[PROC_ID] AS PROC_ID,
    	[PROC_ID_TEXT] AS PROC_ID_TEXT,
    	[PROC_PASSIV] AS PROC_PASSIV,
    	[PROC_TEXT] AS PROC_TEXT
    FROM [utdata].[utdata298].[EK_DIM_OBJ_PROC]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2985')