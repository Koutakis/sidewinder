from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_anltyp",
    source_entity="EK_DIM_OBJ_ANLTYP",
    table="ek_dim_obj_anltyp",
    schema="raindance_raw_8580",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANLTYP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANLTYP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANLTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANLTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANLTYP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANLTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sts', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([ANLTYP_GILTIG_FOM], '1899-12-31 00:00:00') AS ANLTYP_GILTIG_FOM,
    	COALESCE([ANLTYP_GILTIG_TOM], '1899-12-31 00:00:00') AS ANLTYP_GILTIG_TOM,
    	[ANLTYP_ID] AS ANLTYP_ID,
    	[ANLTYP_ID_TEXT] AS ANLTYP_ID_TEXT,
    	[ANLTYP_PASSIV] AS ANLTYP_PASSIV,
    	[ANLTYP_TEXT] AS ANLTYP_TEXT
    FROM [stsudp].[udp_858].[EK_DIM_OBJ_ANLTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8580')