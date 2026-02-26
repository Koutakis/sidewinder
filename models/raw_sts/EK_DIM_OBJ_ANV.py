from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_anv",
    source_entity="EK_DIM_OBJ_ANV",
    table="ek_dim_obj_anv",
    schema="raindance_raw_8580",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANV_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTCE_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAKTCE_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAKTCE_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTCE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKTCE_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FAKTCE_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VERK_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sts', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([ANV_GILTIG_FOM], '1899-12-31 00:00:00') AS ANV_GILTIG_FOM,
    	COALESCE([ANV_GILTIG_TOM], '1899-12-31 00:00:00') AS ANV_GILTIG_TOM,
    	[ANV_ID] AS ANV_ID,
    	[ANV_ID_TEXT] AS ANV_ID_TEXT,
    	[ANV_PASSIV] AS ANV_PASSIV,
    	[ANV_TEXT] AS ANV_TEXT,
    	COALESCE([FAKTCE_GILTIG_FOM], '1899-12-31 00:00:00') AS FAKTCE_GILTIG_FOM,
    	COALESCE([FAKTCE_GILTIG_TOM], '1899-12-31 00:00:00') AS FAKTCE_GILTIG_TOM,
    	[FAKTCE_ID] AS FAKTCE_ID,
    	[FAKTCE_ID_TEXT] AS FAKTCE_ID_TEXT,
    	[FAKTCE_PASSIV] AS FAKTCE_PASSIV,
    	[FAKTCE_TEXT] AS FAKTCE_TEXT,
    	COALESCE([VERK_GILTIG_FOM], '1899-12-31 00:00:00') AS VERK_GILTIG_FOM,
    	COALESCE([VERK_GILTIG_TOM], '1899-12-31 00:00:00') AS VERK_GILTIG_TOM,
    	[VERK_ID] AS VERK_ID,
    	[VERK_ID_TEXT] AS VERK_ID_TEXT,
    	[VERK_PASSIV] AS VERK_PASSIV,
    	[VERK_TEXT] AS VERK_TEXT
    FROM [stsudp].[udp_858].[EK_DIM_OBJ_ANV]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8580')