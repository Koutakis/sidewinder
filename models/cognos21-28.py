from core import run_ingest, TableMode
import polars as pl
from pathlib import Path
from source.cognos.cognos_transform import read_and_transform


def execute(start=None, end=None):
    mega_df = pl.DataFrame()
    for i in [21, 22, 23, 24, 25, 26, 27, 28]: # years to be included
        df = read_and_transform(
            file_path=Path(f"/home/hq0x/ingest-cronjobs/tmp/Test_Cc_utdata{i}.txt"),
            separator="\t",
            encoding="utf-8",
        )
        mega_df = pl.concat([mega_df, df])
    return mega_df


run_ingest(
    dest_env="BIG_EKONOMI_EXECUTION_PROD",
    dest_table="hq0x_sandbox.cognos_trail",
    execute=execute,
    table_mode=TableMode.FULL,
    schedule="0 6 * * *",
    force=True,
)
