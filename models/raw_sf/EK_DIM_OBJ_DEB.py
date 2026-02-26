from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_deb",
    source_entity="EK_DIM_OBJ_DEB",
    table="ek_dim_obj_deb",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DEB_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DEB_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DEB_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="DEB_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="DEB_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="DEB_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([DEB_GILTIG_FOM], '1899-12-31 00:00:00') AS DEB_GILTIG_FOM,
    	COALESCE([DEB_GILTIG_TOM], '1899-12-31 00:00:00') AS DEB_GILTIG_TOM,
    	[DEB_ID] AS DEB_ID,
    	[DEB_ID_TEXT] AS DEB_ID_TEXT,
    	[DEB_PASSIV] AS DEB_PASSIV,
    	[DEB_TEXT] AS DEB_TEXT
    FROM [utdata].[utdata298].[EK_DIM_OBJ_DEB]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2985')