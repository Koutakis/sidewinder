from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_föpro2",
    source_entity="EK_DIM_OBJ_FÖPRO2",
    table="ek_dim_obj_föpro2",
    schema="raindance_raw_8530",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FÖPRO2_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FÖPRO2_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FÖPRO2_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FÖPRO2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FÖPRO2_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FÖPRO2_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ste', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([FÖPRO2_GILTIG_FOM], '1899-12-31 00:00:00') AS FÖPRO2_GILTIG_FOM,
    	COALESCE([FÖPRO2_GILTIG_TOM], '1899-12-31 00:00:00') AS FÖPRO2_GILTIG_TOM,
    	[FÖPRO2_ID] AS FÖPRO2_ID,
    	[FÖPRO2_ID_TEXT] AS FÖPRO2_ID_TEXT,
    	[FÖPRO2_PASSIV] AS FÖPRO2_PASSIV,
    	[FÖPRO2_TEXT] AS FÖPRO2_TEXT
    FROM [steudp].[udp_600].[EK_DIM_OBJ_FÖPRO2]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8530')