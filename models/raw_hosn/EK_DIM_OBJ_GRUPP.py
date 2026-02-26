from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_grupp",
    source_entity="EK_DIM_OBJ_GRUPP",
    table="ek_dim_obj_grupp",
    schema="raindance_raw_1500",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAC_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAC_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FAC_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAC_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAC_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FAC_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="GRUPP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GRUPP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GRUPP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="GRUPP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="GRUPP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="GRUPP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['hosn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([FAC_GILTIG_FOM], '1899-12-31 00:00:00') AS FAC_GILTIG_FOM,
    	COALESCE([FAC_GILTIG_TOM], '1899-12-31 00:00:00') AS FAC_GILTIG_TOM,
    	[FAC_ID] AS FAC_ID,
    	[FAC_ID_TEXT] AS FAC_ID_TEXT,
    	[FAC_PASSIV] AS FAC_PASSIV,
    	[FAC_TEXT] AS FAC_TEXT,
    	COALESCE([GRUPP_GILTIG_FOM], '1899-12-31 00:00:00') AS GRUPP_GILTIG_FOM,
    	COALESCE([GRUPP_GILTIG_TOM], '1899-12-31 00:00:00') AS GRUPP_GILTIG_TOM,
    	[GRUPP_ID] AS GRUPP_ID,
    	[GRUPP_ID_TEXT] AS GRUPP_ID_TEXT,
    	[GRUPP_PASSIV] AS GRUPP_PASSIV,
    	[GRUPP_TEXT] AS GRUPP_TEXT
    FROM [utdata].[utdata150].[EK_DIM_OBJ_GRUPP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_1500')