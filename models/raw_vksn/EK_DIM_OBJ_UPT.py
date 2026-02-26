from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_upt",
    source_entity="EK_DIM_OBJ_UPT",
    table="ek_dim_obj_upt",
    schema="raindance_raw_1550",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UPT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UPT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="UPT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="UPT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="UPT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="UPT_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['vksn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([UPT_GILTIG_FOM], '1899-12-31 00:00:00') AS UPT_GILTIG_FOM,
    	COALESCE([UPT_GILTIG_TOM], '1899-12-31 00:00:00') AS UPT_GILTIG_TOM,
    	[UPT_ID] AS UPT_ID,
    	[UPT_ID_TEXT] AS UPT_ID_TEXT,
    	[UPT_PASSIV] AS UPT_PASSIV,
    	[UPT_TEXT] AS UPT_TEXT
    FROM [utdata].[utdata155].[EK_DIM_OBJ_UPT]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1550')