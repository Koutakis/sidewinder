from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_klinik",
    source_entity="EK_DIM_OBJ_KLINIK",
    table="ek_dim_obj_klinik",
    schema="raindance_raw_8050",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KLINIK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KLINIK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KLINIK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KLINIK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KLINIK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KLINIK_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['tobir', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([KLINIK_GILTIG_FOM], '1899-12-31 00:00:00') AS KLINIK_GILTIG_FOM,
    	COALESCE([KLINIK_GILTIG_TOM], '1899-12-31 00:00:00') AS KLINIK_GILTIG_TOM,
    	[KLINIK_ID] AS KLINIK_ID,
    	[KLINIK_ID_TEXT] AS KLINIK_ID_TEXT,
    	[KLINIK_PASSIV] AS KLINIK_PASSIV,
    	[KLINIK_TEXT] AS KLINIK_TEXT
    FROM [utdata].[utdata805].[EK_DIM_OBJ_KLINIK]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8050')