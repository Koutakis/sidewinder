from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_val",
    source_entity="EK_DIM_OBJ_VAL",
    table="ek_dim_obj_val",
    schema="raindance_raw_8580",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VAL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VAL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VAL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VAL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="VAL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="VAL_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sts', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([VAL_GILTIG_FOM], '1899-12-31 00:00:00') AS VAL_GILTIG_FOM,
    	COALESCE([VAL_GILTIG_TOM], '1899-12-31 00:00:00') AS VAL_GILTIG_TOM,
    	[VAL_ID] AS VAL_ID,
    	[VAL_ID_TEXT] AS VAL_ID_TEXT,
    	[VAL_PASSIV] AS VAL_PASSIV,
    	[VAL_TEXT] AS VAL_TEXT
    FROM [stsudp].[udp_858].[EK_DIM_OBJ_VAL]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8580')