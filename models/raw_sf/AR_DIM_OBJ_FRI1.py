from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ar_dim_obj_fri1",
    source_entity="AR_DIM_OBJ_FRI1",
    table="ar_dim_obj_fri1",
    schema="raindance_raw_2985",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRI1_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRI1_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRI1_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRI1_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRI1_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRI1_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRIGRP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRIGRP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="FRIGRP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRIGRP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="FRIGRP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="FRIGRP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sf', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([FRI1_GILTIG_FOM], '1899-12-31 00:00:00') AS FRI1_GILTIG_FOM,
    	COALESCE([FRI1_GILTIG_TOM], '1899-12-31 00:00:00') AS FRI1_GILTIG_TOM,
    	[FRI1_ID] AS FRI1_ID,
    	[FRI1_ID_TEXT] AS FRI1_ID_TEXT,
    	[FRI1_PASSIV] AS FRI1_PASSIV,
    	[FRI1_TEXT] AS FRI1_TEXT,
    	COALESCE([FRIGRP_GILTIG_FOM], '1899-12-31 00:00:00') AS FRIGRP_GILTIG_FOM,
    	COALESCE([FRIGRP_GILTIG_TOM], '1899-12-31 00:00:00') AS FRIGRP_GILTIG_TOM,
    	[FRIGRP_ID] AS FRIGRP_ID,
    	[FRIGRP_ID_TEXT] AS FRIGRP_ID_TEXT,
    	[FRIGRP_PASSIV] AS FRIGRP_PASSIV,
    	[FRIGRP_TEXT] AS FRIGRP_TEXT
    FROM [utdata].[utdata298].[AR_DIM_OBJ_FRI1]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_2985')