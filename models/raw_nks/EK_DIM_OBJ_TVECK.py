from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_tveck",
    source_entity="EK_DIM_OBJ_TVECK",
    table="ek_dim_obj_tveck",
    schema="raindance_raw_2710",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TVECK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TVECK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TVECK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TVECK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TVECK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="TVECK_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([TVECK_GILTIG_FOM], '1899-12-31 00:00:00') AS TVECK_GILTIG_FOM,
    	COALESCE([TVECK_GILTIG_TOM], '1899-12-31 00:00:00') AS TVECK_GILTIG_TOM,
    	[TVECK_ID] AS TVECK_ID,
    	[TVECK_ID_TEXT] AS TVECK_ID_TEXT,
    	[TVECK_PASSIV] AS TVECK_PASSIV,
    	[TVECK_TEXT] AS TVECK_TEXT
    FROM [raindance_udp].[udp_100].[EK_DIM_OBJ_TVECK]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')