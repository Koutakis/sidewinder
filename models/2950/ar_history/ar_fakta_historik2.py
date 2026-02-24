from bollhav import Model, WriteMode
from core import read
from roskarl import env_var_dsn
import polars as pl


config = Model(
    name="ar_fakta_historik2_2950",
    source_table="AR_FAKTA_HISTORIK2",
    destination_table="ar_fakta_historik2_2950",
    destination_schema="hq0x_sandbox",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns={
        "_data_modified": pl.Date,
        "_metadata_modified": pl.Datetime,
        "ANDEL": pl.Float64,
        "ANLAGGNING": pl.String,
        "ANLTYP_ID": pl.String,
        "ANSK": pl.Float64,
        "BERAKNBELOPP": pl.Float64,
        "FORSALJNINGSBELOPP": pl.Float64,
        "FRI1_ID": pl.String,
        "HANDELSE": pl.String,
        "INTRBELOPP": pl.Float64,
        "KALKBELOPP": pl.Float64,
        "KORRTYP": pl.String,
        "KST_ID": pl.String,
        "PLAC_ID": pl.String,
        "PLAVSKR": pl.Float64,
        "PLMBELOPP": pl.Float64,
        "PROJ_ID": pl.String,
        "REG_DATUM_TID": pl.Datetime,
        "REGSIGN": pl.String,
        "SKMBELOPP": pl.Float64,
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
    yield from read(env_var_dsn("RAINDANCE_2950"), query)
