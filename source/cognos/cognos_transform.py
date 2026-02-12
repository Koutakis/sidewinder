import polars as pl
from pathlib import Path


def _parse_period(perakt: pl.Expr) -> pl.Expr:
    year = pl.lit(2000) + perakt.str.slice(0, 2).cast(pl.Int32)
    month = perakt.str.slice(2, 2).cast(pl.Int32)
    return pl.date(year, month, 1)


def _parse_aktualitet(perakt: pl.Expr) -> pl.Expr:
    return perakt.str.slice(4, 2)


def _extract_bolag_id(col: pl.Expr) -> pl.Expr:
    return (
        pl.when(col.str.ends_with("MA"))
        .then(col.str.replace("MA$", ""))
        .when(col.str.starts_with("TR"))
        .then(col.str.replace("^TR", ""))
        .when(col.str.ends_with("L"))
        .then(col.str.replace("L$", ""))
        .otherwise(col)
    )


def _extract_justeringstyp(col: pl.Expr) -> pl.Expr:
    return (
        pl.when(col.str.ends_with("MA"))
        .then(pl.lit("Manuella Justeringar (MA)"))
        .when(col.str.starts_with("TR"))
        .then(pl.lit("Trafiken operativ (TR)"))
        .when(col.str.ends_with("L"))
        .then(pl.lit("Leasing (L)"))
        .otherwise(pl.lit("Ej justering"))
    )


ANLT_PREFIXES = [
    "1010", "1013", "1110", "1130", "1150", "1215",
    "1220", "1240", "1261", "1262", "1264", "1269", "1270",
]
PANY_PREFIXES = ["1280", "1275"]
LS_PREFIXES = ["2350", "2390"]


def _datatyp_cognos(konto: pl.Expr) -> pl.Expr:
    konto_len = konto.str.len_chars()
    konto_left4 = konto.str.slice(0, 4)
    konto_left3 = konto.str.slice(0, 3)

    is_non_numeric = konto.str.contains(r"[^\d]")

    str_branch = (
        pl.when(konto.str.starts_with("HARB"))
        .then(pl.lit("HARB"))
        .when(konto_len == 6)
        .then(konto_left3)
        .when(konto_len == 7)
        .then(konto_left4)
        .otherwise(konto)
    )

    num_branch = (
        pl.when((konto_len == 7) & konto_left4.is_in(ANLT_PREFIXES))
        .then(pl.lit("ANLT"))
        .when((konto_len == 7) & konto_left4.is_in(PANY_PREFIXES))
        .then(pl.lit("PANY"))
        .when((konto_len == 7) & konto_left4.is_in(LS_PREFIXES))
        .then(pl.lit("LS"))
        .when((konto.cast(pl.Int64, strict=False) < 2000) | (konto == "AJ"))
        .then(pl.lit("B"))
        .otherwise(pl.lit("R"))
    )

    return (
        pl.when(is_non_numeric & konto.is_not_null())
        .then(str_branch)
        .otherwise(num_branch)
        .alias("Datatyp Cognos")
    )


def _parse_belopp(col: pl.Expr) -> pl.Expr:
    return col.str.replace(",", ".").cast(pl.Float64)


def _format_period_us(col: pl.Expr) -> pl.Expr:
    m = col.dt.month().cast(pl.Utf8)
    y = col.dt.year().cast(pl.Utf8)
    return m + "/" + pl.lit("1") + "/" + y


def _clean_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col(pl.Utf8).str.strip_chars().replace("", None)
    )


def read_and_transform(file_path: str | Path, separator: str = "\t", encoding: str = "utf-8") -> pl.DataFrame:
    path = Path(file_path)

    if path.suffix.lower() in (".csv", ".tsv", ".txt"):
        df = pl.read_csv(path, separator=separator, encoding=encoding, infer_schema_length=0)
    elif path.suffix.lower() == ".parquet":
        df = pl.read_parquet(path)
    elif path.suffix.lower() in (".xlsx", ".xls"):
        df = pl.read_excel(path)
    else:
        raise ValueError(f"Unsupported file format: {path.suffix}")

    df = df.rename({c: c.strip() for c in df.columns})
    df = df.with_columns(pl.all().cast(pl.Utf8))
    df = _clean_columns(df)

    df = df.with_row_index("Source Row Number", offset=1)

    period_date = _parse_period(pl.col("perakt"))

    transformed = df.select(
        _datatyp_cognos(pl.col("konto")),
        _extract_justeringstyp(pl.col("motbol")).alias("Justeringstyp motpart"),
        _parse_belopp(pl.col("belopp")).alias("Belopp"),
        pl.col("motbol").fill_null("").alias("Motpartsbolag"),
        _extract_bolag_id(pl.col("bol")).alias("Bolag"),
        _extract_justeringstyp(pl.col("bol")).alias("Justeringstyp"),
        _parse_aktualitet(pl.col("perakt")).alias("Aktualitet"),
        _format_period_us(period_date).alias("Period"),
        period_date.alias("_period_date"),
        pl.col("Source Row Number"),
        pl.col("perakt"),
        pl.col("travkd").fill_null("").alias("Verksamhetsgren"),
        pl.col("ktypkonc").fill_null(""),
        pl.col("vernr").fill_null(""),
        pl.col("vtyp").fill_null(""),
        pl.col("bol"),
        pl.col("konto").alias("Konto"),
    )

    # Accumulated: all rows as-is
    accumulated = transformed.with_columns(pl.lit(True).alias("is_accumulated"))

    # De-accumulated: Ursprung (all 12 months) + Avackumulering (months 1-11 shifted)
    ursprung = transformed.with_columns(pl.lit(False).alias("is_accumulated"))

    non_december = transformed.filter(pl.col("_period_date").dt.month() != 12)
    shifted_date = pl.col("_period_date").dt.offset_by("1mo")
    avack = non_december.with_columns(
        _format_period_us(shifted_date).alias("Period"),
        shifted_date.alias("_period_date"),
        (pl.col("Belopp") * -1).alias("Belopp"),
        pl.lit(False).alias("is_accumulated"),
    )

    result = pl.concat([accumulated, ursprung, avack])

    return result.drop("_period_date")


def one_table_to_rule_them_all(directory: Path) -> pl.DataFrame:
    files = sorted(directory.glob("*.txt"))
    return pl.concat([read_and_transform(f) for f in files])


if __name__ == "__main__":
    import sys

    target = Path(sys.argv[1])

    pl.Config.set_tbl_cols(-1)

    if target.is_dir():
        result_df = one_table_to_rule_them_all(target)
    else:
        result_df = read_and_transform(target)

    print(result_df.head(20))
    print(f"\nRows: {result_df.shape[0]}, Columns: {result_df.shape[1]}")
