from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_ppyrke",
    source_entity="EK_DIM_OBJ_PPYRKE",
    table="ek_dim_obj_ppyrke",
    schema="raindance_raw_8810",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PPYRKE_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PPYRKE_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PPYRKE_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PPYRKE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PPYRKE_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PPYRKE_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ftsl', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PPYRKE_GILTIG_FOM], '1899-12-31 00:00:00') AS PPYRKE_GILTIG_FOM,
    	COALESCE([PPYRKE_GILTIG_TOM], '1899-12-31 00:00:00') AS PPYRKE_GILTIG_TOM,
    	[PPYRKE_ID] AS PPYRKE_ID,
    	[PPYRKE_ID_TEXT] AS PPYRKE_ID_TEXT,
    	[PPYRKE_PASSIV] AS PPYRKE_PASSIV,
    	[PPYRKE_TEXT] AS PPYRKE_TEXT
    FROM [ftvudp].[ftv_400].[EK_DIM_OBJ_PPYRKE]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8810')