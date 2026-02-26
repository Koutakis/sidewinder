from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read

config = Model(
    name="ek_dim_obj_motp",
    source_entity="EK_DIM_OBJ_MOTP",
    table="ek_dim_obj_motp",
    schema="raindance_raw_8580",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTP_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTSLL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTSLL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTSLL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTSLL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTSLL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTSLL_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MPANSV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MPANSV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MPANSV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MPANSV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MPANSV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MPANSV_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sts', 'raindance', 'raw'],
)

def execute(env, cfg=config):
    query=f"""SELECT * FROM (SELECT
    	CAST(GETDATE() AS DATE) as _data_modified,
    	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
    	COALESCE([MOTP_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTP_GILTIG_FOM,
    	COALESCE([MOTP_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTP_GILTIG_TOM,
    	[MOTP_ID] AS MOTP_ID,
    	[MOTP_ID_TEXT] AS MOTP_ID_TEXT,
    	[MOTP_PASSIV] AS MOTP_PASSIV,
    	[MOTP_TEXT] AS MOTP_TEXT,
    	COALESCE([MOTSLL_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTSLL_GILTIG_FOM,
    	COALESCE([MOTSLL_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTSLL_GILTIG_TOM,
    	[MOTSLL_ID] AS MOTSLL_ID,
    	[MOTSLL_ID_TEXT] AS MOTSLL_ID_TEXT,
    	[MOTSLL_PASSIV] AS MOTSLL_PASSIV,
    	[MOTSLL_TEXT] AS MOTSLL_TEXT,
    	COALESCE([MPANSV_GILTIG_FOM], '1899-12-31 00:00:00') AS MPANSV_GILTIG_FOM,
    	COALESCE([MPANSV_GILTIG_TOM], '1899-12-31 00:00:00') AS MPANSV_GILTIG_TOM,
    	[MPANSV_ID] AS MPANSV_ID,
    	[MPANSV_ID_TEXT] AS MPANSV_ID_TEXT,
    	[MPANSV_PASSIV] AS MPANSV_PASSIV,
    	[MPANSV_TEXT] AS MPANSV_TEXT
    FROM [stsudp].[udp_858].[EK_DIM_OBJ_MOTP]) y
    WHERE 1=1"""
    yield from read(query=query, env_var_name='RAINDANCE_8580')