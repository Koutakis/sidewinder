from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_föproc",
    source_entity="EK_DIM_OBJ_FÖPROC",
    table="ek_dim_obj_föproc",
    schema="raindance_raw_2950",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FÖPROC_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FÖPROC_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FÖPROC_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FÖPROC_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FÖPROC_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FÖPROC_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['rlk', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([FÖPROC_GILTIG_FOM], '1899-12-31 00:00:00') AS FÖPROC_GILTIG_FOM,
    	COALESCE([FÖPROC_GILTIG_TOM], '1899-12-31 00:00:00') AS FÖPROC_GILTIG_TOM,
    	[FÖPROC_ID] AS FÖPROC_ID,
    	[FÖPROC_ID_TEXT] AS FÖPROC_ID_TEXT,
    	[FÖPROC_PASSIV] AS FÖPROC_PASSIV,
    	[FÖPROC_TEXT] AS FÖPROC_TEXT
    FROM [utdata].[utdata295].[EK_DIM_OBJ_FÖPROC]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2950')