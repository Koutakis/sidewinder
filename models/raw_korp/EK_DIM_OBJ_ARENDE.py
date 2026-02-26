from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_arende",
    source_entity="EK_DIM_OBJ_ARENDE",
    table="ek_dim_obj_arende",
    schema="raindance_raw_2870",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ARENDE_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ARENDE_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ARENDE_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ARENDE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ARENDE_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ARENDE_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['korp', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([ARENDE_GILTIG_FOM], '1899-12-31 00:00:00') AS ARENDE_GILTIG_FOM,
    	COALESCE([ARENDE_GILTIG_TOM], '1899-12-31 00:00:00') AS ARENDE_GILTIG_TOM,
    	[ARENDE_ID] AS ARENDE_ID,
    	[ARENDE_ID_TEXT] AS ARENDE_ID_TEXT,
    	[ARENDE_PASSIV] AS ARENDE_PASSIV,
    	[ARENDE_TEXT] AS ARENDE_TEXT
    FROM [utdata].[utdata287].[EK_DIM_OBJ_ARENDE]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2870')