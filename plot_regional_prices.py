import pandas as pd
import geopandas as gdp
import matplotlib.pyplot as plt

# read in price data
df_prices = pd.read_csv('data/uk_price_quotes.csv')

def load_shapefile():
    """Load in geospatial data and drop unwanted columns."""
    region_path = "data/geospatial/eurostat_regions/NUTS_RG_2021.shp"

    return (
        gdp.read_file(region_path)[['NUTS_NAME','geometry']]
    )


def filter_uk_regions(df):
    """Filter on UK regions of interest only."""

    regions_list = [
        'London',
        'South East (England)',
        'South West (England)',
        'East Anglia',
        'East Midlands (England)',
        'West Midlands (England)',
        'Yorkshire and the Humber',
        'North West (England)',
        'Wales',
        'Scotland',
        'Northern Ireland',
        'North East (England)'
    ]
    return df.loc[df['NUTS_NAME'].isin(regions_list)]


def agg_northern_regions(df, col):
    """Aggregate northern regions for consistency between datasets"""

    northern_dict = {
        "North West":"North",
        "North West (England)":"North",
        "Yorkshire and Humber":"North",
        "Yorkshire and the Humber":"North",
        "North East (England)":"North"
    }
    return df.replace({col:northern_dict})


df_regions = load_shapefile()
df_regions = filter_uk_regions(df_regions)
df_regions = agg_northern_regions(df_regions, "NUTS_NAME")

df_prices = agg_northern_regions(df_prices, "region")

# df = pd.merge(df_prices, df_regions, left_on='region', right_on='NUTS_NAME')

figsize = (20, 11)
df_regions.plot()
plt.show()