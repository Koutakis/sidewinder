from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_ansvar",
    source_entity="EK_DIM_OBJ_ANSVAR",
    table="ek_dim_obj_ansvar",
    schema="raindance_raw_8570",
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
        PostgresColumn(name="VKO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VKO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VKO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VKO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VKO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VKO_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sos', 'raindance', 'raw'],
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
    	[ANSVAR_TEXT] AS ANSVAR_TEXT,
    	COALESCE([VKO_GILTIG_FOM], '1899-12-31 00:00:00') AS VKO_GILTIG_FOM,
    	COALESCE([VKO_GILTIG_TOM], '1899-12-31 00:00:00') AS VKO_GILTIG_TOM,
    	[VKO_ID] AS VKO_ID,
    	[VKO_ID_TEXT] AS VKO_ID_TEXT,
    	[VKO_PASSIV] AS VKO_PASSIV,
    	[VKO_TEXT] AS VKO_TEXT
    FROM [raindance_udp].[udp_220].[EK_DIM_OBJ_ANSVAR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8570')