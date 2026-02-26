from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_styp",
    source_entity="EK_DIM_OBJ_STYP",
    table="ek_dim_obj_styp",
    schema="raindance_raw_8530",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="STYP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="STYP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="STYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="STYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="STYP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="STYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ste', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([STYP_GILTIG_FOM], '1899-12-31 00:00:00') AS STYP_GILTIG_FOM,
    	COALESCE([STYP_GILTIG_TOM], '1899-12-31 00:00:00') AS STYP_GILTIG_TOM,
    	[STYP_ID] AS STYP_ID,
    	[STYP_ID_TEXT] AS STYP_ID_TEXT,
    	[STYP_PASSIV] AS STYP_PASSIV,
    	[STYP_TEXT] AS STYP_TEXT
    FROM [steudp].[udp_600].[EK_DIM_OBJ_STYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8530')