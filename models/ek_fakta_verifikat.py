from orchestrator import run_ingest, read, TableMode


def execute(start=None, end=None):
    query = f"""
    SELECT * FROM (
        SELECT
            CAST(VERDATUM AS DATE) as _data_modified,
            CAST(GETDATE() AS DATETIME2) as _metadata_modified,
            COALESCE([ATTESTDATUM1], '1899-12-31 00:00:00') AS ATTESTDATUM1,
            COALESCE([ATTESTDATUM2], '1899-12-31 00:00:00') AS ATTESTDATUM2,
            [ATTESTSIGN1] AS ATTESTSIGN1,
            [ATTESTSIGN2] AS ATTESTSIGN2,
            [BUDGET_V] AS BUDGET_V,
            COALESCE([DEFDATUM], '1899-12-31 00:00:00') AS DEFDATUM,
            [DEFSIGN] AS DEFSIGN,
            [DOK_ANTAL] AS DOK_ANTAL,
            [DOKTYP] AS DOKTYP,
            [DOKUMENTID] AS DOKUMENTID,
            [EXTERNANM] AS EXTERNANM,
            [EXTERNID] AS EXTERNID,
            [EXTERNNR] AS EXTERNNR,
            [FORETAG] AS FORETAG,
            [HUVUDTEXT] AS HUVUDTEXT,
            [IB] AS IB,
            [INTERNVERNR] AS INTERNVERNR,
            [KATEGORI] AS KATEGORI,
            [KONTO_ID] AS KONTO_ID,
            [KONTSIGN] AS KONTSIGN,
            [KST_ID] AS KST_ID,
            [MED] AS MED,
            [MOTP_ID] AS MOTP_ID,
            [PNYCKEL] AS PNYCKEL,
            [PRG_V] AS PRG_V,
            [PROJ_ID] AS PROJ_ID,
            [RADTEXT] AS RADTEXT,
            [RADTYPNR] AS RADTYPNR,
            [REGDAT_ID] AS REGDAT_ID,
            COALESCE([REGDATUM], '1899-12-31 00:00:00') AS REGDATUM,
            [REGSIGN] AS REGSIGN,
            [STATUS] AS STATUS,
            [URSPRUNGS_VERIFIKAT] AS URSPRUNGS_VERIFIKAT,
            [URSPTEXT] AS URSPTEXT,
            [UTFALL_V] AS UTFALL_V,
            [UTILITY] AS UTILITY,
            [UTLVAL_V] AS UTLVAL_V,
            [VALUTA_ID] AS VALUTA_ID,
            COALESCE([VERDATUM], '1899-12-31 00:00:00') AS VERDATUM,
            [VERDOKREF] AS VERDOKREF,
            [VERNR] AS VERNR,
            [VERRAD] AS VERRAD,
            [VERTYP] AS VERTYP
        FROM [utdata].[utdata361].[EK_FAKTA_VERIFIKAT]
    ) y
    WHERE _data_modified BETWEEN '{start}' AND '{end}'
    """
    return read("RAINDANCE_3610", query)


run_ingest(
    dest_env="BIG_EKONOMI_EXECUTION_PROD",
    dest_table="hq0x_sandbox.ek_fakta_verifikat",
    execute=execute,
    table_mode=TableMode.INCREMENTAL,
    schedule="0 6 * * *",
    start="2024-01-01",
    end="2024-06-30",
    force=True,
)
