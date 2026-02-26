from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ar_dim_obj_ansvar",
    source_entity="AR_DIM_OBJ_ANSVAR",
    table="ar_dim_obj_ansvar",
    schema="raindance_raw_8580",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSVAR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSVAR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANSVAR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSVAR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANSVAR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANSVAR_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sts', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([ANSVAR_GILTIG_FOM], '1899-12-31 00:00:00') AS ANSVAR_GILTIG_FOM,
    	COALESCE([ANSVAR_GILTIG_TOM], '1899-12-31 00:00:00') AS ANSVAR_GILTIG_TOM,
    	[ANSVAR_ID] AS ANSVAR_ID,
    	[ANSVAR_ID_TEXT] AS ANSVAR_ID_TEXT,
    	[ANSVAR_PASSIV] AS ANSVAR_PASSIV,
    	[ANSVAR_TEXT] AS ANSVAR_TEXT
    FROM [stsudp].[udp_858].[AR_DIM_OBJ_ANSVAR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8580')