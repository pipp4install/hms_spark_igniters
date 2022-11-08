from pyspark.sql import SparkSession, functions as F

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


def sum_price(df):
    """Sum the price over each unique product and region."""
    return (
        df
        .groupBy('quote_date', 'item_id', 'region').sum('price')
        .withColumnRenamed('sum(price)','price')
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

url = "https://raw.githubusercontent.com/pipp4install/hms_spark_igniters/main/pricequotes202209.csv"

df = load_data(url)
df = sum_price(df)
df = decode_region(df)
