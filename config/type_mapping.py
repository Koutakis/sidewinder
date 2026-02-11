import polars as pl


POLARS_TO_PG: dict[type, str] = {
    pl.String: "TEXT",
    pl.Int8: "SMALLINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INTEGER",
    pl.Int64: "BIGINT",
    pl.Float32: "REAL",
    pl.Float64: "DOUBLE PRECISION",
    pl.Boolean: "BOOLEAN",
    pl.Date: "DATE",
    pl.Datetime: "TIMESTAMPTZ",
    pl.Time: "TIME",
    pl.Decimal: "NUMERIC",
    pl.Binary: "BYTEA",
}


def pg_type_from_polars(dtype: pl.DataType) -> str:
    return POLARS_TO_PG.get(type(dtype), "TEXT")
