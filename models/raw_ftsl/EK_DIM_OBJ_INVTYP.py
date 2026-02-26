from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_invtyp",
    source_entity="EK_DIM_OBJ_INVTYP",
    table="ek_dim_obj_invtyp",
    schema="raindance_raw_8810",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="INVTYP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="INVTYP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="INVTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="INVTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="INVTYP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="INVTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ftsl', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([INVTYP_GILTIG_FOM], '1899-12-31 00:00:00') AS INVTYP_GILTIG_FOM,
    	COALESCE([INVTYP_GILTIG_TOM], '1899-12-31 00:00:00') AS INVTYP_GILTIG_TOM,
    	[INVTYP_ID] AS INVTYP_ID,
    	[INVTYP_ID_TEXT] AS INVTYP_ID_TEXT,
    	[INVTYP_PASSIV] AS INVTYP_PASSIV,
    	[INVTYP_TEXT] AS INVTYP_TEXT
    FROM [ftvudp].[ftv_400].[EK_DIM_OBJ_INVTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8810')