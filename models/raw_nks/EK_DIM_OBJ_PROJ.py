from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_proj",
    source_entity="EK_DIM_OBJ_PROJ",
    table="ek_dim_obj_proj",
    schema="raindance_raw_nks",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROGR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROGR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROGR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROGR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROGR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROGR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJ_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROJ_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROJ_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROTOT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROTOT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PROTOT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROTOT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROTOT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PROTOT_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['nks', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PROGR_GILTIG_FOM], '1899-12-31 00:00:00') AS PROGR_GILTIG_FOM,
    	COALESCE([PROGR_GILTIG_TOM], '1899-12-31 00:00:00') AS PROGR_GILTIG_TOM,
    	[PROGR_ID] AS PROGR_ID,
    	[PROGR_ID_TEXT] AS PROGR_ID_TEXT,
    	[PROGR_PASSIV] AS PROGR_PASSIV,
    	[PROGR_TEXT] AS PROGR_TEXT,
    	COALESCE([PROJ_GILTIG_FOM], '1899-12-31 00:00:00') AS PROJ_GILTIG_FOM,
    	COALESCE([PROJ_GILTIG_TOM], '1899-12-31 00:00:00') AS PROJ_GILTIG_TOM,
    	[PROJ_ID] AS PROJ_ID,
    	[PROJ_ID_TEXT] AS PROJ_ID_TEXT,
    	[PROJ_PASSIV] AS PROJ_PASSIV,
    	[PROJ_TEXT] AS PROJ_TEXT,
    	COALESCE([PROTOT_GILTIG_FOM], '1899-12-31 00:00:00') AS PROTOT_GILTIG_FOM,
    	COALESCE([PROTOT_GILTIG_TOM], '1899-12-31 00:00:00') AS PROTOT_GILTIG_TOM,
    	[PROTOT_ID] AS PROTOT_ID,
    	[PROTOT_ID_TEXT] AS PROTOT_ID_TEXT,
    	[PROTOT_PASSIV] AS PROTOT_PASSIV,
    	[PROTOT_TEXT] AS PROTOT_TEXT
    FROM [raindance_udp].[udp_100].[EK_DIM_OBJ_PROJ]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2710')