from pathlib import Path
import glob

import pandas as pd


def load_data(filepath):
    """Import csv data from GitHub repo."""
    df = pd.read_csv(filepath)
    df.columns = [col.lower() for col in df.columns]
    df = df[["quote_date", "item_id", "item_desc", "price", "region"]]
    df["item_id"] = df["item_id"].astype("str")
    return df


def sum_price(df):
    """Sum the price over each unique product and region."""
    return df.groupby(["quote_date", "item_id", "region"], as_index=False)[
        "price"
    ].mean()


region_map_dict = {
    1: "catalogue_collections",
    2: "London",
    3: "South East",
    4: "South West",
    5: "East Anglia",
    6: "East Midlands",
    7: "West Midlands",
    8: "Yorkshire and Humber",
    9: "North West",
    10: "North",
    11: "Wales",
    12: "Scotland",
    13: "Northern Ireland",
}


def decode_region(df):
    """Apply mapping to update the region code to region."""
    df["region"] = df["region"].replace(region_map_dict)
    return df


def run_clean_and_read_products(filepath) -> pd.DataFrame():
    """Clean and Read Products Prices for Month"""

    df = load_data(filepath)
    df = sum_price(df)
    df = decode_region(df)

    return df


def apply_mappers(df) -> pd.DataFrame:
    """
    Apply:
        1) Apply CPI Map. Item ID -> COICOP4 ID
        2) Apply COICOP Category Map. COICOP4 ID -> COICOP4 Description.
    """

    cpi_map = pd.read_csv(
        "./data/mappers/cpi_classification_framework_2022.csv", dtype="str"
    )
    cpi_map = cpi_map[["item_id", "coicop4_id"]]

    coicop_cat_map = pd.read_csv("./data/mappers/coicop_category_map.csv", dtype="str")

    df = df.merge(cpi_map, on="item_id", how="left")
    df = df.merge(coicop_cat_map, on="coicop4_id", how="left")

    return df


def get_prices_filenames() -> list[str]:
    return [Path(file) for file in glob.glob("./data/prices/*.csv")]


if __name__ == "main":

    prices_filenames = get_prices_filenames()

    # Read & Process Each Price File
    dfs = []
    for price_file in prices_filenames:
        df = run_clean_and_read_products(price_file)
        df = apply_mappers(df)
        dfs.append(df)

    # Union dfs
    df = pd.concat(dfs)

    # Average Price Per Period By Region and Category
    df = df.groupby(["quote_date", "region", "desc"], as_index=False)["price"].mean()
    df["quote_date"] = pd.to_datetime(df["quote_date"], format="%Y%m")

    # Save CSV
    df.to_csv("./data/processed/prices_processed_by_category_region.csv", index=False)
