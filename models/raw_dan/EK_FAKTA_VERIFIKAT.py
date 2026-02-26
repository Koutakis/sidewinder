from bollhav import Model, WriteMode
from bollhav.postgres import PostgresColumn, PostgresType
from bollhav.database import Database
from core import read, write
from roskarl.marshal import with_env_config, EnvConfig
from roskarl import env_var_dsn

config = Model(
    name="ek_fakta_verifikat",
    source_entity="EK_FAKTA_VERIFIKAT",
    table="ek_fakta_verifikat",
    schema="raindance_raw_8510",
    write_mode=WriteMode.MERGE,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTESTDATUM1", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTESTDATUM2", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTESTSIGN1", data_type=PostgresType.TEXT),
        PostgresColumn(name="ATTESTSIGN2", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVTAL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="BOK_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="BPUTF_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="BUD_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="DEFANL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="DEFDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DEFSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="DOK_ANTAL", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="DOKTYP", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="DOKUMENTID", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNANM", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNID", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNNR", data_type=PostgresType.TEXT),
        PostgresColumn(name="FAKT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FORETAG", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="FÖPROC_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FÖRBEL_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="HUVUDTEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="HÄND_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="IB", data_type=PostgresType.TEXT),
        PostgresColumn(name="INTERNVERNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KASSA_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KATEGORI", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KONTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="LEVID_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MED", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="MOTP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MPAYID_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ORGVAL_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="PNYCKEL", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RADTEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RADTYPNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="REGDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="REGSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="STATUS", data_type=PostgresType.TEXT),
        PostgresColumn(name="URSPR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="URSPRUNGS_VERIFIKAT", data_type=PostgresType.TEXT),
        PostgresColumn(name="URSPTEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="UTF_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="UTILITY", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VALUTA_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERDOKREF", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VERRAD", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VERTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="XFAKT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="XLEVID_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="YG_ID", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['dan', 'raindance', 'raw'],
)

@with_env_config
def execute(env: EnvConfig, cfg=config):
    dest_dsn = env_var_dsn("BIG_EKONOMI_EXECUTION_PROD")
    if env.backfill and env.backfill.enabled:
        since = env.backfill.since.strftime("%Y-%m-%d")
        until = env.backfill.until.strftime("%Y-%m-%d")
    elif env.cron and env.cron.enabled:
        since = env.cron.since.strftime("%Y-%m-%d")
        until = env.cron.until.strftime("%Y-%m-%d")
    else:
        raise ValueError(f"{cfg.name}: MERGE requires CRON or BACKFILL env vars")
    query = f"""
    SELECT
	CAST(VERDATUM AS DATE) as _data_modified,
	CAST(GETDATE() AS DATETIME2) as _metadata_modified,
	COALESCE([ATTESTDATUM1], '1899-12-31 00:00:00') AS ATTESTDATUM1,
	COALESCE([ATTESTDATUM2], '1899-12-31 00:00:00') AS ATTESTDATUM2,
	[ATTESTSIGN1] AS ATTESTSIGN1,
	[ATTESTSIGN2] AS ATTESTSIGN2,
	[AVTAL_ID] AS AVTAL_ID,
	[BOK_V] AS BOK_V,
	[BPUTF_V] AS BPUTF_V,
	[BUD_V] AS BUD_V,
	[DEFANL_ID] AS DEFANL_ID,
	COALESCE([DEFDATUM], '1899-12-31 00:00:00') AS DEFDATUM,
	[DEFSIGN] AS DEFSIGN,
	[DOK_ANTAL] AS DOK_ANTAL,
	[DOKTYP] AS DOKTYP,
	[DOKUMENTID] AS DOKUMENTID,
	[EXTERNANM] AS EXTERNANM,
	[EXTERNID] AS EXTERNID,
	[EXTERNNR] AS EXTERNNR,
	[FAKT_ID] AS FAKT_ID,
	[FORETAG] AS FORETAG,
	[FÖPROC_ID] AS FÖPROC_ID,
	[FÖRBEL_V] AS FÖRBEL_V,
	[HUVUDTEXT] AS HUVUDTEXT,
	[HÄND_ID] AS HÄND_ID,
	[IB] AS IB,
	[INTERNVERNR] AS INTERNVERNR,
	[KASSA_ID] AS KASSA_ID,
	[KATEGORI] AS KATEGORI,
	[KONTO_ID] AS KONTO_ID,
	[KONTSIGN] AS KONTSIGN,
	[KST_ID] AS KST_ID,
	[LEVID_ID] AS LEVID_ID,
	[MED] AS MED,
	[MOTP_ID] AS MOTP_ID,
	[MPAYID_ID] AS MPAYID_ID,
	[ORGVAL_V] AS ORGVAL_V,
	[PNYCKEL] AS PNYCKEL,
	[PROJ_ID] AS PROJ_ID,
	[RADTEXT] AS RADTEXT,
	[RADTYPNR] AS RADTYPNR,
	COALESCE([REGDATUM], '1899-12-31 00:00:00') AS REGDATUM,
	[REGSIGN] AS REGSIGN,
	[STATUS] AS STATUS,
	[URSPR_ID] AS URSPR_ID,
	[URSPRUNGS_VERIFIKAT] AS URSPRUNGS_VERIFIKAT,
	[URSPTEXT] AS URSPTEXT,
	[UTF_V] AS UTF_V,
	[UTILITY] AS UTILITY,
	[VALUTA_ID] AS VALUTA_ID,
	COALESCE([VERDATUM], '1899-12-31 00:00:00') AS VERDATUM,
	[VERDOKREF] AS VERDOKREF,
	[VERNR] AS VERNR,
	[VERRAD] AS VERRAD,
	[VERTYP] AS VERTYP,
	[XFAKT_ID] AS XFAKT_ID,
	[XLEVID_ID] AS XLEVID_ID,
	[YG_ID] AS YG_ID
    FROM [raindance_udp].[udp_150].[EK_FAKTA_VERIFIKAT]
    WHERE CAST(VERDATUM AS DATE) BETWEEN '{since}' AND '{until}'
    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_8510", query, batch_size=500_000):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn, since=since, until=until)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")