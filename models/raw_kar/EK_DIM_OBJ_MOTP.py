from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_dim_obj_motp",
    source_entity="EK_DIM_OBJ_MOTP",
    table="ek_dim_obj_motp",
    schema="raindance_raw_1210",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTDIV_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTDIV_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTDIV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTDIV_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTDIV_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTDIV_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTKLI_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTKLI_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTKLI_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTKLI_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTKLI_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTKLI_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTP_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTSEK_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTSEK_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTSEK_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTSEK_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTSEK_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTSEK_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTSLL_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTSLL_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTSLL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTSLL_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTSLL_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTSLL_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([MOTDIV_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTDIV_GILTIG_FOM,
	COALESCE([MOTDIV_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTDIV_GILTIG_TOM,
	[MOTDIV_ID] AS MOTDIV_ID,
	[MOTDIV_ID_TEXT] AS MOTDIV_ID_TEXT,
	[MOTDIV_PASSIV] AS MOTDIV_PASSIV,
	[MOTDIV_TEXT] AS MOTDIV_TEXT,
	COALESCE([MOTKLI_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTKLI_GILTIG_FOM,
	COALESCE([MOTKLI_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTKLI_GILTIG_TOM,
	[MOTKLI_ID] AS MOTKLI_ID,
	[MOTKLI_ID_TEXT] AS MOTKLI_ID_TEXT,
	[MOTKLI_PASSIV] AS MOTKLI_PASSIV,
	[MOTKLI_TEXT] AS MOTKLI_TEXT,
	COALESCE([MOTP_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTP_GILTIG_FOM,
	COALESCE([MOTP_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTP_GILTIG_TOM,
	[MOTP_ID] AS MOTP_ID,
	[MOTP_ID_TEXT] AS MOTP_ID_TEXT,
	[MOTP_PASSIV] AS MOTP_PASSIV,
	[MOTP_TEXT] AS MOTP_TEXT,
	COALESCE([MOTSEK_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTSEK_GILTIG_FOM,
	COALESCE([MOTSEK_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTSEK_GILTIG_TOM,
	[MOTSEK_ID] AS MOTSEK_ID,
	[MOTSEK_ID_TEXT] AS MOTSEK_ID_TEXT,
	[MOTSEK_PASSIV] AS MOTSEK_PASSIV,
	[MOTSEK_TEXT] AS MOTSEK_TEXT,
	COALESCE([MOTSLL_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTSLL_GILTIG_FOM,
	COALESCE([MOTSLL_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTSLL_GILTIG_TOM,
	[MOTSLL_ID] AS MOTSLL_ID,
	[MOTSLL_ID_TEXT] AS MOTSLL_ID_TEXT,
	[MOTSLL_PASSIV] AS MOTSLL_PASSIV,
	[MOTSLL_TEXT] AS MOTSLL_TEXT
    FROM [Utdata].[udp_100].[EK_DIM_OBJ_MOTP]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_1210", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")