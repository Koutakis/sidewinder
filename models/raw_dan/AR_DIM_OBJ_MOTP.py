from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ar_dim_obj_motp",
    source_entity="AR_DIM_OBJ_MOTP",
    table="ar_dim_obj_motp",
    schema="raindance_raw_8510",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KMOTP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KMOTP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="KMOTP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KMOTP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="KMOTP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="KMOTP_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MGRP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MGRP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MGRP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MGRP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MGRP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MGRP_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTP_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([KMOTP_GILTIG_FOM], '1899-12-31 00:00:00') AS KMOTP_GILTIG_FOM,
    	COALESCE([KMOTP_GILTIG_TOM], '1899-12-31 00:00:00') AS KMOTP_GILTIG_TOM,
    	[KMOTP_ID] AS KMOTP_ID,
    	[KMOTP_ID_TEXT] AS KMOTP_ID_TEXT,
    	[KMOTP_PASSIV] AS KMOTP_PASSIV,
    	[KMOTP_TEXT] AS KMOTP_TEXT,
    	COALESCE([MGRP_GILTIG_FOM], '1899-12-31 00:00:00') AS MGRP_GILTIG_FOM,
    	COALESCE([MGRP_GILTIG_TOM], '1899-12-31 00:00:00') AS MGRP_GILTIG_TOM,
    	[MGRP_ID] AS MGRP_ID,
    	[MGRP_ID_TEXT] AS MGRP_ID_TEXT,
    	[MGRP_PASSIV] AS MGRP_PASSIV,
    	[MGRP_TEXT] AS MGRP_TEXT,
    	COALESCE([MOTP_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTP_GILTIG_FOM,
    	COALESCE([MOTP_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTP_GILTIG_TOM,
    	[MOTP_ID] AS MOTP_ID,
    	[MOTP_ID_TEXT] AS MOTP_ID_TEXT,
    	[MOTP_PASSIV] AS MOTP_PASSIV,
    	[MOTP_TEXT] AS MOTP_TEXT
    FROM [raindance_udp].[udp_150].[AR_DIM_OBJ_MOTP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8510')