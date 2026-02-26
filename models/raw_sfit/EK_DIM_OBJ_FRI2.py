from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_fri2",
    source_entity="EK_DIM_OBJ_FRI2",
    table="ek_dim_obj_fri2",
    schema="raindance_raw_2940",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRI2_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRI2_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRI2_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRI2_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRI2_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRI2_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sfit', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([FRI2_GILTIG_FOM], '1899-12-31 00:00:00') AS FRI2_GILTIG_FOM,
    	COALESCE([FRI2_GILTIG_TOM], '1899-12-31 00:00:00') AS FRI2_GILTIG_TOM,
    	[FRI2_ID] AS FRI2_ID,
    	[FRI2_ID_TEXT] AS FRI2_ID_TEXT,
    	[FRI2_PASSIV] AS FRI2_PASSIV,
    	[FRI2_TEXT] AS FRI2_TEXT
    FROM [utdata].[utdata294].[EK_DIM_OBJ_FRI2]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2940')