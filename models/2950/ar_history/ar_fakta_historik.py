from bollhav import Model, WriteMode
from core import read
import polars as pl


config = Model(
    name="ar_fakta_historik_2950",
    source_table="AR_FAKTA_HISTORIK",
    destination_table="ar_fakta_historik_2950",
    destination_schema="hq0x_sandbox",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns={
        "_data_modified": pl.Date,
        "_metadata_modified": pl.Datetime,
        "ANLAGGNING": pl.String,
        "ANLTYP_ID": pl.String,
        "ANSK": pl.Float64,
        "BERAKNBELOPP": pl.Float64,
        "HANDELSE": pl.String,
        "KORRTYP": pl.String,
        "KST_ID": pl.String,
        "PLAC2_ID": pl.String,
        "PLAC_ID": pl.String,
        "PLAVSKR": pl.Float64,
        "PROJ_ID": pl.String,
        "REG_DATUM_TID": pl.Datetime,
        "REGSIGN": pl.String,
        "UTILITY": pl.String,
        "VERDATUM": pl.Datetime,
        "VERNR": pl.Int64,
    },
    cron="0 6 * * *",
    tags=["raindance"],
)


def execute(env, cfg=config):
    query = """
    SELECT
        CAST(GETDATE() AS DATE) as _data_modified,
        CAST(GETDATE() AS DATETIME2) as _metadata_modified,
        [ANLAGGNING] AS ANLAGGNING,
        [ANLTYP_ID] AS ANLTYP_ID,
        [ANSK] AS ANSK,
        [BERAKNBELOPP] AS BERAKNBELOPP,
        [HANDELSE] AS HANDELSE,
        [KORRTYP] AS KORRTYP,
        [KST_ID] AS KST_ID,
        [PLAC2_ID] AS PLAC2_ID,
        [PLAC_ID] AS PLAC_ID,
        [PLAVSKR] AS PLAVSKR,
        [PROJ_ID] AS PROJ_ID,
        [REG_DATUM_TID] AS REG_DATUM_TID,
        [REGSIGN] AS REGSIGN,
        [UTILITY] AS UTILITY,
        COALESCE([VERDATUM], '1899-12-31 00:00:00') AS VERDATUM,
        [VERNR] AS VERNR
    FROM [utdata].[utdata295].[AR_FAKTA_HISTORIK]
    """
    yield from read(env_name="RAINDANCE_2950", query=query)
