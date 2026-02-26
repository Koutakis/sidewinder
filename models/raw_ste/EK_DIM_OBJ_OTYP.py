from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_otyp",
    source_entity="EK_DIM_OBJ_OTYP",
    table="ek_dim_obj_otyp",
    schema="raindance_raw_8530",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OTYP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OTYP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="OTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="OTYP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="OTYP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="OTYP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['ste', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([OTYP_GILTIG_FOM], '1899-12-31 00:00:00') AS OTYP_GILTIG_FOM,
    	COALESCE([OTYP_GILTIG_TOM], '1899-12-31 00:00:00') AS OTYP_GILTIG_TOM,
    	[OTYP_ID] AS OTYP_ID,
    	[OTYP_ID_TEXT] AS OTYP_ID_TEXT,
    	[OTYP_PASSIV] AS OTYP_PASSIV,
    	[OTYP_TEXT] AS OTYP_TEXT
    FROM [steudp].[udp_600].[EK_DIM_OBJ_OTYP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8530')