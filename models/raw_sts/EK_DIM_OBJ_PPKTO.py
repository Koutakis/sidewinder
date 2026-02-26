from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_ppkto",
    source_entity="EK_DIM_OBJ_PPKTO",
    table="ek_dim_obj_ppkto",
    schema="raindance_raw_8580",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PPKTO_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PPKTO_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PPKTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PPKTO_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PPKTO_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PPKTO_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sts', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PPKTO_GILTIG_FOM], '1899-12-31 00:00:00') AS PPKTO_GILTIG_FOM,
    	COALESCE([PPKTO_GILTIG_TOM], '1899-12-31 00:00:00') AS PPKTO_GILTIG_TOM,
    	[PPKTO_ID] AS PPKTO_ID,
    	[PPKTO_ID_TEXT] AS PPKTO_ID_TEXT,
    	[PPKTO_PASSIV] AS PPKTO_PASSIV,
    	[PPKTO_TEXT] AS PPKTO_TEXT
    FROM [stsudp].[udp_858].[EK_DIM_OBJ_PPKTO]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8580')