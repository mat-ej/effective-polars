import polars as pl
import polars.selectors as cs

def tweak_auto(df: pl.DataFrame):
    cols = ['year', 'make', 'model', 'displ', 'cylinders', 'trany',
            'drive', 'VClass', 'fuelType', 'barrels08', 'city08',
            'highway08', 'createdOn']

    return (df
        .select(pl.col(cols))
        .with_columns(
            pl.col('year').cast(pl.Int16),
            pl.col(['cylinders', 'highway08', 'city08']).cast(pl.UInt8),
            pl.col(['displ', 'barrels08']).cast(pl.Float32),
            pl.col(['make', 'model', 'VClass', 'drive', 'fuelType']).cast(pl.Categorical),
            pl.col('createdOn').str.to_datetime('%a %b %d %H:%M:%S %Z %Y'),
            is_automatic=pl.col('trany').str.to_lowercase().str.contains('automatic').fill_null(True),
            num_gears=pl.col('trany').str.extract(r'(\d+)').cast(pl.UInt8).fill_null(6)
            )
        )