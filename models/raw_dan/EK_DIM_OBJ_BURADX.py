from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_buradx",
    source_entity="EK_DIM_OBJ_BURADX",
    table="ek_dim_obj_buradx",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BURADX_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BURADX_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="BURADX_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="BURADX_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="BURADX_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="BURADX_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([BURADX_GILTIG_FOM], '1899-12-31 00:00:00') AS BURADX_GILTIG_FOM,
    	COALESCE([BURADX_GILTIG_TOM], '1899-12-31 00:00:00') AS BURADX_GILTIG_TOM,
    	[BURADX_ID] AS BURADX_ID,
    	[BURADX_ID_TEXT] AS BURADX_ID_TEXT,
    	[BURADX_PASSIV] AS BURADX_PASSIV,
    	[BURADX_TEXT] AS BURADX_TEXT
    FROM [raindance_udp].[udp_150].[EK_DIM_OBJ_BURADX]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')