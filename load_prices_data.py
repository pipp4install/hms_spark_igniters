from functools import reduce

from pyspark.sql import (
    DataFrame,
    functions as F,
    SparkSession,

)


spark = (
    SparkSession.builder.appName("default-session")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)


def load_data(url):
    """Import csv data from GitHub repo."""
    return (
        spark.read.csv(url, header=True)
        .select('quote_date', 'item_id', 'item_desc', 'price', 'region')
        .withColumn('quote_date', F.to_date(F.col('quote_date'), "yyyyMM"))
        .withColumn('price', F.col('price').cast('float'))
    )


def average_price(df):
    """Sum the price over each unique product and region."""
    return (
        df
        .groupBy('quote_date', 'item_id', 'region').avg('price')
        .withColumnRenamed('avg(price)','price')
        .withColumn('price', F.round(F.col('price'), scale=2))
    )


def decode_region(df):
    """Apply mapping to update the region code to region."""
    mapping = (
        F.when(F.col('region') == "1", "catalogue_collections")
        .when(F.col('region') == "2", "London")
        .when(F.col('region') == "3", "South East")
        .when(F.col('region') == "4", "South West")
        .when(F.col('region') == "5", "East Anglia")
        .when(F.col('region') == "6", "East Midlands")
        .when(F.col('region') == "7", "West Midlands")
        .when(F.col('region') == "8", "Yorkshire and Humber")
        .when(F.col('region') == "9", "North West")
        .when(F.col('region') == "10", "North")
        .when(F.col('region') == "11", "Wales")
        .when(F.col('region') == "12", "Scotland")
        .when(F.col('region') == "13", "Northern Ireland")
        .otherwise(None)
    )

    return (
        df
        .withColumn('region', mapping)
        .dropna(subset=['region'])
    )


def load_and_clean_data(urls):
    """Load csv files, clean and concat into one dataframe."""
    dfs = []

    for url in urls:
        df = load_data(url)
        df = average_price(df)
        df = decode_region(df)
        dfs.append(df)

    return reduce(DataFrame.unionByName, dfs)


github = "https://raw.githubusercontent.com/"
repo = "pipp4install/hms_spark_igniters/main/data/prices/"
urls = [
    f"{github}{repo}202001.csv",
    f"{github}{repo}202002.csv",
    f"{github}{repo}202003.csv",
    f"{github}{repo}202004.csv",
    f"{github}{repo}202005.csv",
    f"{github}{repo}202006.csv",
    f"{github}{repo}202007.csv",
    f"{github}{repo}202008.csv",
    f"{github}{repo}202009.csv",
    f"{github}{repo}202010.csv",
    f"{github}{repo}202011.csv",
    f"{github}{repo}202012.csv",
    f"{github}{repo}202101.csv",
    f"{github}{repo}202102.csv",
    f"{github}{repo}202103.csv",
    f"{github}{repo}202104.csv",
    f"{github}{repo}202105.csv",
    f"{github}{repo}202106.csv",
    f"{github}{repo}202107.csv",
    f"{github}{repo}202108.csv",
    f"{github}{repo}202109.csv",
    f"{github}{repo}202110.csv",
    f"{github}{repo}202111.csv",
    f"{github}{repo}202112.csv",
    f"{github}{repo}202201.csv",
    f"{github}{repo}202202.csv",
    f"{github}{repo}202203.csv",
    f"{github}{repo}202204.csv",
    f"{github}{repo}202205.csv",
    f"{github}{repo}202206.csv",
    f"{github}{repo}202207.csv",
    f"{github}{repo}202208.csv",
    f"{github}{repo}202209.csv",
]

df = load_and_clean_data(urls)
