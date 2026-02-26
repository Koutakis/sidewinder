from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_avt",
    source_entity="EK_DIM_OBJ_AVT",
    table="ek_dim_obj_avt",
    schema="raindance_raw_1560",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AVT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="AVT_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SHA_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SHA_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SHA_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SHA_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SHA_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SHA_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['pvn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([AVT_GILTIG_FOM], '1899-12-31 00:00:00') AS AVT_GILTIG_FOM,
    	COALESCE([AVT_GILTIG_TOM], '1899-12-31 00:00:00') AS AVT_GILTIG_TOM,
    	[AVT_ID] AS AVT_ID,
    	[AVT_ID_TEXT] AS AVT_ID_TEXT,
    	[AVT_PASSIV] AS AVT_PASSIV,
    	[AVT_TEXT] AS AVT_TEXT,
    	COALESCE([SHA_GILTIG_FOM], '1899-12-31 00:00:00') AS SHA_GILTIG_FOM,
    	COALESCE([SHA_GILTIG_TOM], '1899-12-31 00:00:00') AS SHA_GILTIG_TOM,
    	[SHA_ID] AS SHA_ID,
    	[SHA_ID_TEXT] AS SHA_ID_TEXT,
    	[SHA_PASSIV] AS SHA_PASSIV,
    	[SHA_TEXT] AS SHA_TEXT
    FROM [utdata].[utdata156].[EK_DIM_OBJ_AVT]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1560')