from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_proj",
    source_entity="EK_DIM_OBJ_PROJ",
    table="ek_dim_obj_proj",
    schema="raindance_raw_8050",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJ_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJ_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJ_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROJ_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['tobir', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PROJ_GILTIG_FOM], '1899-12-31 00:00:00') AS PROJ_GILTIG_FOM,
    	COALESCE([PROJ_GILTIG_TOM], '1899-12-31 00:00:00') AS PROJ_GILTIG_TOM,
    	[PROJ_ID] AS PROJ_ID,
    	[PROJ_ID_TEXT] AS PROJ_ID_TEXT,
    	[PROJ_PASSIV] AS PROJ_PASSIV,
    	[PROJ_TEXT] AS PROJ_TEXT
    FROM [utdata].[utdata805].[EK_DIM_OBJ_PROJ]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8050')