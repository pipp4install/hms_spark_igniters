from functools import reduce

from pyspark.sql import (
    DataFrame,
    functions as F,
    SparkSession,
    Window,
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


def apply_coicop_mapper(df, mapper):
    """Reduce the item id to coicop4"""
    return (
        df
        .join(mapper, on='item_id', how='left')
        .withColumnRenamed('desc', 'product')
        .select('quote_date', 'item_id', 'product', 'region', 'price', )
    )


def calculate_price_change(df):
    """Calculate the month on month price differences."""
    previous_month = (
        Window.partitionBy('region', 'item_id')
        .orderBy('quote_date')
    )

    calc_price_difference = (
        F.when(F.col('prev_price').isNotNull(),
               F.col('price') - F.col('prev_price'))
        .otherwise(F.lit(None))
    )

    round_differences = (
         F.when(F.col('price_diff').isNotNull(),
                F.round(F.col('price_diff'), scale=2))
        .otherwise(F.lit(None))
    )

    return (
        df
        # Create a column with the previous month's price.
        .withColumn('prev_price', F.lag('price').over(previous_month))
        # Create a column containing the price differences.
        .withColumn('price_diff', calc_price_difference)
        .withColumn('price_diff', round_differences)
    )


github = "https://raw.githubusercontent.com/"
repo = "pipp4install/hms_spark_igniters/main/data/"
urls = [
    f"{github}{repo}prices/202001.csv",
    f"{github}{repo}prices/202002.csv",
    f"{github}{repo}prices/202003.csv",
    f"{github}{repo}prices/202004.csv",
    f"{github}{repo}prices/202005.csv",
    f"{github}{repo}prices/202006.csv",
    f"{github}{repo}prices/202007.csv",
    f"{github}{repo}prices/202008.csv",
    f"{github}{repo}prices/202009.csv",
    f"{github}{repo}prices/202010.csv",
    f"{github}{repo}prices/202011.csv",
    f"{github}{repo}prices/202012.csv",
    f"{github}{repo}prices/202101.csv",
    f"{github}{repo}prices/202102.csv",
    f"{github}{repo}prices/202103.csv",
    f"{github}{repo}prices/202104.csv",
    f"{github}{repo}prices/202105.csv",
    f"{github}{repo}prices/202106.csv",
    f"{github}{repo}prices/202107.csv",
    f"{github}{repo}prices/202108.csv",
    f"{github}{repo}prices/202109.csv",
    f"{github}{repo}prices/202110.csv",
    f"{github}{repo}prices/202111.csv",
    f"{github}{repo}prices/202112.csv",
    f"{github}{repo}prices/202201.csv",
    f"{github}{repo}prices/202202.csv",
    f"{github}{repo}prices/202203.csv",
    f"{github}{repo}prices/202204.csv",
    f"{github}{repo}prices/202205.csv",
    f"{github}{repo}prices/202206.csv",
    f"{github}{repo}prices/202207.csv",
    f"{github}{repo}prices/202208.csv",
    f"{github}{repo}prices/202209.csv",
]

df = load_and_clean_data(urls)

mapper = spark.read.csv(
    f"{github}{repo}coicop_mapper.csv",
    header=True,
)
df = apply_coicop_mapper(df, mapper)

df = calculate_price_change(df)
