from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_yrkgr",
    source_entity="EK_DIM_OBJ_YRKGR",
    table="ek_dim_obj_yrkgr",
    schema="raindance_raw_8530",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="YRKGR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="YRKGR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="YRKGR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="YRKGR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="YRKGR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="YRKGR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="YRTOT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="YRTOT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="YRTOT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="YRTOT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="YRTOT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="YRTOT_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ste', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([YRKGR_GILTIG_FOM], '1899-12-31 00:00:00') AS YRKGR_GILTIG_FOM,
    	COALESCE([YRKGR_GILTIG_TOM], '1899-12-31 00:00:00') AS YRKGR_GILTIG_TOM,
    	[YRKGR_ID] AS YRKGR_ID,
    	[YRKGR_ID_TEXT] AS YRKGR_ID_TEXT,
    	[YRKGR_PASSIV] AS YRKGR_PASSIV,
    	[YRKGR_TEXT] AS YRKGR_TEXT,
    	COALESCE([YRTOT_GILTIG_FOM], '1899-12-31 00:00:00') AS YRTOT_GILTIG_FOM,
    	COALESCE([YRTOT_GILTIG_TOM], '1899-12-31 00:00:00') AS YRTOT_GILTIG_TOM,
    	[YRTOT_ID] AS YRTOT_ID,
    	[YRTOT_ID_TEXT] AS YRTOT_ID_TEXT,
    	[YRTOT_PASSIV] AS YRTOT_PASSIV,
    	[YRTOT_TEXT] AS YRTOT_TEXT
    FROM [steudp].[udp_600].[EK_DIM_OBJ_YRKGR]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8530')