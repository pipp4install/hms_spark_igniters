import streamlit as st
from streamlit_option_menu import option_menu

import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide")


with st.sidebar:
    selected = option_menu(
        menu_title="Main Menu",
        options=["About", "UK Subnational Prices"],
        icons=["house", "currency-pound"],
    )

if selected == "About":
    st.title(f":memo: {selected}")
    st.text("Streamlit App created for purposes of visualising UK Subnational Product Prices.")
    st.markdown("[Github Repo](https://github.com/pipp4install/hms_spark_igniters)")
    st.markdown("[ONS Prices Data Source](https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes)")

if selected == "UK Subnational Prices":
    st.title(f":pound: {selected}")

    df = pd.read_csv(
        "./data/processed/prices_processed_by_category_region.csv",
        infer_datetime_format=True,
    )
    df = df.rename({"desc": "Category"}, axis=1)
    df.columns = [col.title().replace("_", " ") for col in df.columns]

    st.sidebar.header("Please Filter by Product Category Here:")
    coicop_category = st.sidebar.selectbox(
        label="Categories",
        options=df["Category"].unique(),
        label_visibility="hidden",
    )
    df_selection = df.query(f"Category == @coicop_category")

    st.markdown(
        f"## :chart_with_upwards_trend: Viewing Prices Time Series Filtered by {coicop_category}"
    )
    product_region_plot = px.line(
        df.query("Category == @coicop_category"),
        x="Quote Date",
        y="Price",
        color="Region",
    )
    st.plotly_chart(product_region_plot)

    st.markdown(f"## :book: Viewing DataFrame Filtered by {coicop_category}")
    st.dataframe(df_selection)

    @st.cache
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv(index=False).encode("utf-8")

    csv = convert_df(df_selection)
    st.download_button(
        label="Download Data as CSV",
        data=csv,
        file_name=f"prices_{coicop_category}_by_region_time_series.csv",
        mime="text/csv",
    )
