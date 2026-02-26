from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_ppatyp",
    source_entity="EK_DIM_OBJ_PPATYP",
    table="ek_dim_obj_ppatyp",
    schema="raindance_raw_8580",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PPATYP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PPATYP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="PPATYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PPATYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="PPATYP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="PPATYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sts', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([PPATYP_GILTIG_FOM], '1899-12-31 00:00:00') AS PPATYP_GILTIG_FOM,
    	COALESCE([PPATYP_GILTIG_TOM], '1899-12-31 00:00:00') AS PPATYP_GILTIG_TOM,
    	[PPATYP_ID] AS PPATYP_ID,
    	[PPATYP_ID_TEXT] AS PPATYP_ID_TEXT,
    	[PPATYP_PASSIV] AS PPATYP_PASSIV,
    	[PPATYP_TEXT] AS PPATYP_TEXT
    FROM [stsudp].[udp_858].[EK_DIM_OBJ_PPATYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8580')