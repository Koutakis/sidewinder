from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_kund",
    source_entity="EK_DIM_OBJ_KUND",
    table="ek_dim_obj_kund",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KUND_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KUND_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KUND_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KUND_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KUND_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KUND_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([KUND_GILTIG_FOM], '1899-12-31 00:00:00') AS KUND_GILTIG_FOM,
    	COALESCE([KUND_GILTIG_TOM], '1899-12-31 00:00:00') AS KUND_GILTIG_TOM,
    	[KUND_ID] AS KUND_ID,
    	[KUND_ID_TEXT] AS KUND_ID_TEXT,
    	[KUND_PASSIV] AS KUND_PASSIV,
    	[KUND_TEXT] AS KUND_TEXT
    FROM [utdata].[utdata298].[EK_DIM_OBJ_KUND]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2985')