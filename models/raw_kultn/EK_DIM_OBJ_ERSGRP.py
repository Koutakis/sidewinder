from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_ersgrp",
    source_entity="EK_DIM_OBJ_ERSGRP",
    table="ek_dim_obj_ersgrp",
    schema="raindance_raw_3610",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ERSGRP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ERSGRP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ERSGRP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ERSGRP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="ERSGRP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="ERSGRP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kultn', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([ERSGRP_GILTIG_FOM], '1899-12-31 00:00:00') AS ERSGRP_GILTIG_FOM,
    	COALESCE([ERSGRP_GILTIG_TOM], '1899-12-31 00:00:00') AS ERSGRP_GILTIG_TOM,
    	[ERSGRP_ID] AS ERSGRP_ID,
    	[ERSGRP_ID_TEXT] AS ERSGRP_ID_TEXT,
    	[ERSGRP_PASSIV] AS ERSGRP_PASSIV,
    	[ERSGRP_TEXT] AS ERSGRP_TEXT
    FROM [utdata].[utdata361].[EK_DIM_OBJ_ERSGRP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_3610')