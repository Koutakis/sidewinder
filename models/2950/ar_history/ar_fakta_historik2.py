from core import model, read, TableMode


@model(
    name="ar_fakta_historik2_2950",
    source_table="AR_FAKTA_HISTORIK2",
    destination_table="ar_fakta_historik2_2950",
    destination_schema="hq0x_sandbox",
    dest_env="BIG_EKONOMI_EXECUTION_PROD",
    source_env="RAINDANCE_2950",
    default_table_mode=TableMode.TRUNCATE_INSERT,
)
def execute(cfg):
    query = """
    SELECT
        CAST(GETDATE() AS DATE) as _data_modified,
        CAST(GETDATE() AS DATETIME2) as _metadata_modified,
        [ANDEL] AS ANDEL,
        [ANLAGGNING] AS ANLAGGNING,
        [ANLTYP_ID] AS ANLTYP_ID,
        [ANSK] AS ANSK,
        [BERAKNBELOPP] AS BERAKNBELOPP,
        [FORSALJNINGSBELOPP] AS FORSALJNINGSBELOPP,
        [FRI1_ID] AS FRI1_ID,
        [HANDELSE] AS HANDELSE,
        [INTRBELOPP] AS INTRBELOPP,
        [KALKBELOPP] AS KALKBELOPP,
        [KORRTYP] AS KORRTYP,
        [KST_ID] AS KST_ID,
        [PLAC_ID] AS PLAC_ID,
        [PLAVSKR] AS PLAVSKR,
        [PLMBELOPP] AS PLMBELOPP,
        [PROJ_ID] AS PROJ_ID,
        [REG_DATUM_TID] AS REG_DATUM_TID,
        [REGSIGN] AS REGSIGN,
        [SKMBELOPP] AS SKMBELOPP,
        [UTILITY] AS UTILITY,
        COALESCE([VERDATUM], '1899-12-31 00:00:00') AS VERDATUM,
        [VERNR] AS VERNR
    FROM [utdata].[utdata298].[AR_FAKTA_HISTORIK2]
    """
    yield from read(cfg.source_env, query)


execute()
