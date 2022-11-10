import plotly.express as px
import pandas as pd

df = pd.read_csv("./data/processed/prices_processed_by_category_region.csv", infer_datetime_format=True)
# Beer Average Price Per Region
px.line(df.query("desc == 'Beer'"), x="quote_date", y="price", color="region")