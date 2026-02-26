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
    schema="raindance_raw_1210",
    write_mode=WriteMode.MERGE,
    columns=[
        PostgresColumn(name="_data_modified", data_type=PostgresType.DATE),
        PostgresColumn(name="_metadata_modified", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="AKT_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ANTAL_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="ATTESTDATUM1", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTESTDATUM2", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="ATTESTSIGN1", data_type=PostgresType.TEXT),
        PostgresColumn(name="ATTESTSIGN2", data_type=PostgresType.TEXT),
        PostgresColumn(name="AVTAL_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="BPUTF_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="DEFDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="DEFSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="DOK_ANTAL", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="DOKTYP", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="DOKUMENTID", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNANM", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNID", data_type=PostgresType.TEXT),
        PostgresColumn(name="EXTERNNR", data_type=PostgresType.TEXT),
        PostgresColumn(name="FORETAG", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="FÖPROC_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="FÖRBEL_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="HDATUM_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="HUVUDTEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="IB", data_type=PostgresType.TEXT),
        PostgresColumn(name="INTERNVERNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="INVNR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KATEGORI", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="KONTO_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="KONTSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="KST_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="LEVID_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="MED", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="MOTP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="OKI_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="ORGVAL_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="PNYCKEL", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="PROJ_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RAD_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="RADTEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="RADTYPNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="REGDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="REGSIGN", data_type=PostgresType.TEXT),
        PostgresColumn(name="SKI_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="STATUS", data_type=PostgresType.TEXT),
        PostgresColumn(name="TANV_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TIMMAR_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="TTYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="TYP_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="URSPR_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="URSPRUNGS_VERIFIKAT", data_type=PostgresType.TEXT),
        PostgresColumn(name="URSPTEXT", data_type=PostgresType.TEXT),
        PostgresColumn(name="UTFALL_V", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="UTILITY", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VALUTA_ID", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERDATUM", data_type=PostgresType.TIMESTAMPTZ),
        PostgresColumn(name="VERDOKREF", data_type=PostgresType.TEXT),
        PostgresColumn(name="VERNR", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VERRAD", data_type=PostgresType.NUMERIC),
        PostgresColumn(name="VERTYP", data_type=PostgresType.TEXT),
        PostgresColumn(name="YRKE_ID", data_type=PostgresType.TEXT),
    ],
    database=Database.POSTGRES,
    cron="0 6 * * *",
    tags=['kar', 'raindance', 'raw'],
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
	[AKT_ID] AS AKT_ID,
	[ANTAL_V] AS ANTAL_V,
	COALESCE([ATTESTDATUM1], '1899-12-31 00:00:00') AS ATTESTDATUM1,
	COALESCE([ATTESTDATUM2], '1899-12-31 00:00:00') AS ATTESTDATUM2,
	[ATTESTSIGN1] AS ATTESTSIGN1,
	[ATTESTSIGN2] AS ATTESTSIGN2,
	[AVTAL_ID] AS AVTAL_ID,
	[BPUTF_V] AS BPUTF_V,
	COALESCE([DEFDATUM], '1899-12-31 00:00:00') AS DEFDATUM,
	[DEFSIGN] AS DEFSIGN,
	[DOK_ANTAL] AS DOK_ANTAL,
	[DOKTYP] AS DOKTYP,
	[DOKUMENTID] AS DOKUMENTID,
	[EXTERNANM] AS EXTERNANM,
	[EXTERNID] AS EXTERNID,
	[EXTERNNR] AS EXTERNNR,
	[FORETAG] AS FORETAG,
	[FÖPROC_ID] AS FÖPROC_ID,
	[FÖRBEL_V] AS FÖRBEL_V,
	[HDATUM_ID] AS HDATUM_ID,
	[HUVUDTEXT] AS HUVUDTEXT,
	[IB] AS IB,
	[INTERNVERNR] AS INTERNVERNR,
	[INVNR_ID] AS INVNR_ID,
	[KATEGORI] AS KATEGORI,
	[KONTO_ID] AS KONTO_ID,
	[KONTSIGN] AS KONTSIGN,
	[KST_ID] AS KST_ID,
	[LEVID_ID] AS LEVID_ID,
	[MED] AS MED,
	[MOTP_ID] AS MOTP_ID,
	[OKI_ID] AS OKI_ID,
	[ORGVAL_V] AS ORGVAL_V,
	[PNYCKEL] AS PNYCKEL,
	[PROD_ID] AS PROD_ID,
	[PROJ_ID] AS PROJ_ID,
	[RAD_ID] AS RAD_ID,
	[RADTEXT] AS RADTEXT,
	[RADTYPNR] AS RADTYPNR,
	COALESCE([REGDATUM], '1899-12-31 00:00:00') AS REGDATUM,
	[REGSIGN] AS REGSIGN,
	[SKI_ID] AS SKI_ID,
	[STATUS] AS STATUS,
	[TANV_ID] AS TANV_ID,
	[TIMMAR_V] AS TIMMAR_V,
	[TTYP_ID] AS TTYP_ID,
	[TYP_ID] AS TYP_ID,
	[URSPR_ID] AS URSPR_ID,
	[URSPRUNGS_VERIFIKAT] AS URSPRUNGS_VERIFIKAT,
	[URSPTEXT] AS URSPTEXT,
	[UTFALL_V] AS UTFALL_V,
	[UTILITY] AS UTILITY,
	[VALUTA_ID] AS VALUTA_ID,
	COALESCE([VERDATUM], '1899-12-31 00:00:00') AS VERDATUM,
	[VERDOKREF] AS VERDOKREF,
	[VERNR] AS VERNR,
	[VERRAD] AS VERRAD,
	[VERTYP] AS VERTYP,
	[YRKE_ID] AS YRKE_ID
    FROM [Utdata].[udp_100].[EK_FAKTA_VERIFIKAT]
    WHERE CAST(VERDATUM AS DATE) BETWEEN '{since}' AND '{until}'
    """
    total_rows = 0
    first_batch = True
    for df in read("RAINDANCE_1210", query, batch_size=500_000):
        if len(df) == 0:
            continue
        write(cfg, df, dest_dsn, since=since, until=until)
        if first_batch:
            cfg.write_mode = WriteMode.APPEND
            first_batch = False
        total_rows += len(df)
    print(f"  ✓ {cfg.name}: {total_rows:,} rows written" if total_rows else f"  ⏭ {cfg.name}: no data, skipping")