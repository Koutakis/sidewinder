from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_mpayid",
    source_entity="EK_DIM_OBJ_MPAYID",
    table="ek_dim_obj_mpayid",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MPAYID_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MPAYID_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MPAYID_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MPAYID_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MPAYID_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MPAYID_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([MPAYID_GILTIG_FOM], '1899-12-31 00:00:00') AS MPAYID_GILTIG_FOM,
    	COALESCE([MPAYID_GILTIG_TOM], '1899-12-31 00:00:00') AS MPAYID_GILTIG_TOM,
    	[MPAYID_ID] AS MPAYID_ID,
    	[MPAYID_ID_TEXT] AS MPAYID_ID_TEXT,
    	[MPAYID_PASSIV] AS MPAYID_PASSIV,
    	[MPAYID_TEXT] AS MPAYID_TEXT
    FROM [raindance_udp].[udp_150].[EK_DIM_OBJ_MPAYID]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')