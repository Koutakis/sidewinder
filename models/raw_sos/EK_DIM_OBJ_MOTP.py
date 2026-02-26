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
    schema="raindance_raw_8570",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MIE_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MIE_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MIE_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MIE_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MIE_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MIE_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTP_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTP_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTPGR_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTPGR_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="MOTPGR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTPGR_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="MOTPGR_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="MOTPGR_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SMOT_GILTIG_FOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SMOT_GILTIG_TOM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="SMOT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="SMOT_ID_TEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="SMOT_PASSIV", data_type=PostgresType.BOOLEAN),
        PostgresColumn(name="SMOT_TEXT", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['sos', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    query = """
    SELECT
	CAST(GETDATE() AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([MIE_GILTIG_FOM], '1899-12-31 00:00:00') AS MIE_GILTIG_FOM,
	COALESCE([MIE_GILTIG_TOM], '1899-12-31 00:00:00') AS MIE_GILTIG_TOM,
	[MIE_ID] AS MIE_ID,
	[MIE_ID_TEXT] AS MIE_ID_TEXT,
	[MIE_PASSIV] AS MIE_PASSIV,
	[MIE_TEXT] AS MIE_TEXT,
	COALESCE([MOTP_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTP_GILTIG_FOM,
	COALESCE([MOTP_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTP_GILTIG_TOM,
	[MOTP_ID] AS MOTP_ID,
	[MOTP_ID_TEXT] AS MOTP_ID_TEXT,
	[MOTP_PASSIV] AS MOTP_PASSIV,
	[MOTP_TEXT] AS MOTP_TEXT,
	COALESCE([MOTPGR_GILTIG_FOM], '1899-12-31 00:00:00') AS MOTPGR_GILTIG_FOM,
	COALESCE([MOTPGR_GILTIG_TOM], '1899-12-31 00:00:00') AS MOTPGR_GILTIG_TOM,
	[MOTPGR_ID] AS MOTPGR_ID,
	[MOTPGR_ID_TEXT] AS MOTPGR_ID_TEXT,
	[MOTPGR_PASSIV] AS MOTPGR_PASSIV,
	[MOTPGR_TEXT] AS MOTPGR_TEXT,
	COALESCE([SMOT_GILTIG_FOM], '1899-12-31 00:00:00') AS SMOT_GILTIG_FOM,
	COALESCE([SMOT_GILTIG_TOM], '1899-12-31 00:00:00') AS SMOT_GILTIG_TOM,
	[SMOT_ID] AS SMOT_ID,
	[SMOT_ID_TEXT] AS SMOT_ID_TEXT,
	[SMOT_PASSIV] AS SMOT_PASSIV,
	[SMOT_TEXT] AS SMOT_TEXT
    FROM [raindance_udp].[udp_220].[EK_DIM_OBJ_MOTP]

    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8570", query):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")