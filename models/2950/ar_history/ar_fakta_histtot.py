from bollhav import Model, WriteMode
from core import read
from roskarl import env_var_dsn
import polars as pl


config = Model(
    name="ar_fakta_histtot_2950",
    source_table="AR_FAKTA_HISTTOT",
    destination_table="ar_fakta_histtot_2950",
    destination_schema="hq0x_sandbox",
    write_mode=WriteMode.TRUNCATE_INSERT,
    columns={
        "_data_modified": pl.Date,
        "_metadata_modified": pl.Datetime,
        "ANLTYP_ID": pl.String,
        "BELOPPSTYP": pl.String,
        "FLY": pl.Float64,
        "FORS": pl.Float64,
        "IB": pl.Float64,
        "INV": pl.Float64,
        "KOR": pl.Float64,
        "KST_ID": pl.String,
        "OMF": pl.Float64,
        "PAV": pl.Float64,
        "PERIOD": pl.Datetime,
        "PLAC2_ID": pl.String,
        "PLAC_ID": pl.String,
        "PROJ_ID": pl.String,
        "STR": pl.Float64,
        "UB": pl.Float64,
        "UTF": pl.Float64,
        "UTILITY": pl.String,
        "UTR": pl.Float64,
    },
    cron="0 6 * * *",
    tags=["raindance"],
)


def execute(env, cfg=config):
    query = """
    SELECT
        CAST(GETDATE() AS DATE) as _data_modified,
        CAST(GETDATE() AS DATETIME2) as _metadata_modified,
        [ANLTYP_ID] AS ANLTYP_ID,
        [BELOPPSTYP] AS BELOPPSTYP,
        [FLY] AS FLY,
        [FORS] AS FORS,
        [IB] AS IB,
        [INV] AS INV,
        [KOR] AS KOR,
        [KST_ID] AS KST_ID,
        [OMF] AS OMF,
        [PAV] AS PAV,
        COALESCE([PERIOD], '1899-12-31 00:00:00') AS PERIOD,
        [PLAC2_ID] AS PLAC2_ID,
        [PLAC_ID] AS PLAC_ID,
        [PROJ_ID] AS PROJ_ID,
        [STR] AS STR,
        [UB] AS UB,
        [UTF] AS UTF,
        [UTILITY] AS UTILITY,
        [UTR] AS UTR
    FROM [utdata].[utdata295].[AR_FAKTA_HISTTOT]
    """
    yield from read(env_name="RAINDANCE_2950", query=query)
