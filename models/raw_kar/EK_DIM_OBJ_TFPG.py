from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_tfpg",
    source_entity="EK_DIM_OBJ_TFPG",
    table="ek_dim_obj_tfpg",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TFPG_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TFPG_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="TFPG_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TFPG_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="TFPG_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="TFPG_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([TFPG_GILTIG_FOM], '1899-12-31 00:00:00') AS TFPG_GILTIG_FOM,
    	COALESCE([TFPG_GILTIG_TOM], '1899-12-31 00:00:00') AS TFPG_GILTIG_TOM,
    	[TFPG_ID] AS TFPG_ID,
    	[TFPG_ID_TEXT] AS TFPG_ID_TEXT,
    	[TFPG_PASSIV] AS TFPG_PASSIV,
    	[TFPG_TEXT] AS TFPG_TEXT
    FROM [Utdata].[udp_100].[EK_DIM_OBJ_TFPG]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1210')