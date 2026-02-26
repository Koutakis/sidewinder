from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_anst",
    source_entity="EK_DIM_OBJ_ANST",
    table="ek_dim_obj_anst",
    schema="raindance_raw_8090",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANST_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANST_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ANST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANST_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANST_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ANST_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['medic', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([ANST_GILTIG_FOM], '1899-12-31 00:00:00') AS ANST_GILTIG_FOM,
    	COALESCE([ANST_GILTIG_TOM], '1899-12-31 00:00:00') AS ANST_GILTIG_TOM,
    	[ANST_ID] AS ANST_ID,
    	[ANST_ID_TEXT] AS ANST_ID_TEXT,
    	[ANST_PASSIV] AS ANST_PASSIV,
    	[ANST_TEXT] AS ANST_TEXT
    FROM [MediCarrierUDP].[utdata100].[EK_DIM_OBJ_ANST]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8090')