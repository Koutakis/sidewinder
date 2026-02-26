from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="obj_kst_datben",
    source_entity="OBJ_KST_DATBEN",
    table="obj_kst_datben",
    schema="raindance_raw_2940",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DATUM_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DATUM_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TEXT2", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sfit', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([DATUM_FOM], '1899-12-31 00:00:00') AS DATUM_FOM,
    	COALESCE([DATUM_TOM], '1899-12-31 00:00:00') AS DATUM_TOM,
    	[ID] AS ID,
    	[TEXT2] AS TEXT2
    FROM [utdata].[utdata294].[OBJ_KST_DATBEN]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2940')