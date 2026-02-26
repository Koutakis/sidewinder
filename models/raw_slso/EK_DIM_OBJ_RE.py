from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_re",
    source_entity="EK_DIM_OBJ_RE",
    table="ek_dim_obj_re",
    schema="raindance_raw_1100",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RE_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RE_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="RE_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RE_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="RE_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SA_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SA_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SA_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SA_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SA_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SA_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['slso', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([RE_GILTIG_FOM], '1899-12-31 00:00:00') AS RE_GILTIG_FOM,
    	COALESCE([RE_GILTIG_TOM], '1899-12-31 00:00:00') AS RE_GILTIG_TOM,
    	[RE_ID] AS RE_ID,
    	[RE_ID_TEXT] AS RE_ID_TEXT,
    	[RE_PASSIV] AS RE_PASSIV,
    	[RE_TEXT] AS RE_TEXT,
    	COALESCE([SA_GILTIG_FOM], '1899-12-31 00:00:00') AS SA_GILTIG_FOM,
    	COALESCE([SA_GILTIG_TOM], '1899-12-31 00:00:00') AS SA_GILTIG_TOM,
    	[SA_ID] AS SA_ID,
    	[SA_ID_TEXT] AS SA_ID_TEXT,
    	[SA_PASSIV] AS SA_PASSIV,
    	[SA_TEXT] AS SA_TEXT
    FROM [udpb4].[udpb4_100].[EK_DIM_OBJ_RE]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1100')