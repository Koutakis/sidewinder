from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_pklini",
    source_entity="EK_DIM_OBJ_PKLINI",
    table="ek_dim_obj_pklini",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PKLINI_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PKLINI_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PKLINI_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PKLINI_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PKLINI_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PKLINI_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PKLINI_GILTIG_FOM], '1899-12-31 00:00:00') AS PKLINI_GILTIG_FOM,
    	COALESCE([PKLINI_GILTIG_TOM], '1899-12-31 00:00:00') AS PKLINI_GILTIG_TOM,
    	[PKLINI_ID] AS PKLINI_ID,
    	[PKLINI_ID_TEXT] AS PKLINI_ID_TEXT,
    	[PKLINI_PASSIV] AS PKLINI_PASSIV,
    	[PKLINI_TEXT] AS PKLINI_TEXT
    FROM [Utdata].[udp_100].[EK_DIM_OBJ_PKLINI]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')