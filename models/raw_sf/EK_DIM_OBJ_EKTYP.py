from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_ektyp",
    source_entity="EK_DIM_OBJ_EKTYP",
    table="ek_dim_obj_ektyp",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="EKTYP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="EKTYP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="EKTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="EKTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="EKTYP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="EKTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([EKTYP_GILTIG_FOM], '1899-12-31 00:00:00') AS EKTYP_GILTIG_FOM,
    	COALESCE([EKTYP_GILTIG_TOM], '1899-12-31 00:00:00') AS EKTYP_GILTIG_TOM,
    	[EKTYP_ID] AS EKTYP_ID,
    	[EKTYP_ID_TEXT] AS EKTYP_ID_TEXT,
    	[EKTYP_PASSIV] AS EKTYP_PASSIV,
    	[EKTYP_TEXT] AS EKTYP_TEXT
    FROM [utdata].[utdata298].[EK_DIM_OBJ_EKTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2985')