from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="obj_verdat",
    source_entity="OBJ_VERDAT",
    table="obj_verdat",
    schema="raindance_raw_2990",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['skade', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([GILTIG_FOM], '1899-12-31 00:00:00') AS GILTIG_FOM,
    	COALESCE([GILTIG_TOM], '1899-12-31 00:00:00') AS GILTIG_TOM,
    	[ID] AS ID,
    	[PASSIV] AS PASSIV,
    	[TEXT] AS TEXT
    FROM [utdata].[utdata299].[OBJ_VERDAT]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2990')