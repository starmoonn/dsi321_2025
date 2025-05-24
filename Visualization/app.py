import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from zoneinfo import ZoneInfo
from datetime import timedelta, datetime
import streamlit as st
import streamlit.components.v1 as components
import folium
import s3fs
import geopandas as gpd
import pandas as pd
from datetime import datetime
from shapely.geometry import Point
from folium.plugins import HeatMap
from streamlit_folium import folium_static
import os

# Set up environments of LakeFS
lakefs_endpoint = "http://lakefs-dev:8000/"
ACCESS_KEY = "access_key"
SECRET_KEY = "secret_key"

# Setting S3FileSystem for access LakeFS
fs = {
    "key": ACCESS_KEY,
    "secret": SECRET_KEY,
    "client_kwargs": {'endpoint_url': lakefs_endpoint}
}

# ---------- Load shapefile ----------
@st.cache_data
def load_shapefile():
    gdf = gpd.read_file("shapefile/tha_admbnda_adm1_rtsd_20220121.shp")
    gdf = gdf.drop(columns=gdf.select_dtypes(include=['datetime64']).columns)
    gdf = gdf.to_crs(epsg=4326)
    return gdf

# ---------- Load all parquet files ----------
@st.cache_data
def query_parquet_data():
    firms_lakefs_path = "s3://weather/main/firms.parquet"
    df_firms = pd.read_parquet(    
                path=firms_lakefs_path,
                storage_options=fs
)
    df_firms.drop_duplicates(inplace=True)
    df_firms['acq_date'] = pd.to_datetime(df_firms['acq_date']).dt.date

    return df_firms

# ---------- cache filtered result ----------
@st.cache_data
def filter_by_date(mode, date_start, date_end, date_exact):
    df = query_parquet_data()
    if mode == 'latest':
        return df[df['acq_date'] == df['acq_date'].max()]
    elif mode == 'range':
        return df[(df['acq_date'] >= date_start) & (df['acq_date'] <= date_end)]
    else:
        return df[df['acq_date'] == date_exact]

# ---------- Generate Heatmap ----------
def generate_heatmap(filter_mode, date_start, date_end, date_exact, gdf, df_all):
    # Filter
    df_filtered = df_all

    # Prepare heat data
    heat_data = []
    if 'latitude' in df_filtered.columns and 'longitude' in df_filtered.columns and not df_filtered.empty:
        df_filtered['brightness'] = df_filtered.get('brightness', 1)
        heat_data = df_filtered[['latitude', 'longitude', 'brightness']].values.tolist()
        

    # Folium Map Init
    mymap = folium.Map(
        location=[13.7367, 100.5231],
        zoom_start=6,
        tiles='CartoDB Dark_Matter'
    )


    # GeoDataFrame of fire points
    heat_points = gpd.GeoDataFrame(
        geometry=[Point(lon, lat) for lat, lon, _ in heat_data],
        crs="EPSG:4326"
    )

    # Spatial Join to provinces
    joined = gpd.sjoin(heat_points, gdf, how="left", predicate="within")
    province_counts = joined['ADM1_TH'].value_counts().reset_index()
    province_counts.columns = ['ADM1_TH', 'heat_spot_count']

    # Merge to shapefile
    gdf = gdf.merge(province_counts, on='ADM1_TH', how='left')
    gdf['heat_spot_count'] = gdf['heat_spot_count'].fillna(0).astype(int)

    # Draw Province Layer
    folium.GeoJson(
        gdf,
        name='Provinces',
        tooltip=folium.GeoJsonTooltip(fields=['ADM1_TH', 'heat_spot_count'], aliases=['Province', 'Heat Spots']),
        style_function=lambda x: {
            'fillColor': '#e3e0de',
            'color': 'black',
            'weight': 0.7,
            'fillOpacity': 0.3
        }
    ).add_to(mymap)

    # Normalize heat
    min_b, max_b = 250, 400
    normalized = []
    for lat, lon, b in heat_data:
        b = max(min_b, min(b, max_b))
        weight = (b - min_b) / (max_b - min_b)
        normalized.append([lat, lon, weight])

    # HeatMap Layer
    if normalized:
        HeatMap(
            normalized,
            radius=10,
            blur=15,
            max_zoom=7,
            gradient = {
                "0.1": "#ffffcc",  # Pale yellow
                "0.2": "#ffeda0",
                "0.3": "#fed976",
                "0.4": "#feb24c",
                "0.5": "#fd693c",
                "0.6": "#fc2a2a",
                "0.7": "#e31a1c",
                "0.8": "#bd0000",
                "0.9": "#800000",
                "1.0": "#4d0000",  # Deep red
            }
        ).add_to(mymap)

    return mymap, province_counts.sort_values("heat_spot_count", ascending=False)

# ---------- Streamlit UI ----------
def main():
    st.set_page_config(layout="wide")   

    # Sidebar
    st.sidebar.title("🔍 Filter Options")
    filter_mode = st.sidebar.radio("Filter Mode", ['exact', 'range'])

    # Get the latest date available in the dataset
    if 'latest_date' not in st.session_state:
        latest_df = filter_by_date( mode='latest', date_exact=None, date_start=None, date_end=None)
        latest_date = latest_df['acq_date'].max()  # Get the latest date from the dataset
        st.session_state.latest_date = latest_date
    latest_date = st.session_state.latest_date

    # Date input logic based on filter mode
    if filter_mode == 'range':
        start_date = st.sidebar.date_input("Start Date", latest_date)
        end_date = st.sidebar.date_input("End Date", latest_date)
        exact_date = None
    else:
        exact_date = st.sidebar.date_input("Exact Date", latest_date)
        start_date = end_date = None

    # Load shapefile once
    if 'gdf' not in st.session_state:
        st.session_state.gdf = load_shapefile()
    gdf = st.session_state.gdf

    # Generate a session key based on filter input
    key = f"{filter_mode}_{str(start_date)}_{str(end_date)}_{str(exact_date)}"

    # Cache query result
    if key not in st.session_state:
        st.session_state[key] = filter_by_date(filter_mode, start_date, end_date, exact_date)

    df_all = st.session_state[key]

    # Heatmap
    mymap, province_counts = generate_heatmap(filter_mode, start_date, end_date, exact_date, gdf, df_all)
    map_html = mymap.get_root().render()

    # Table Overlay
    table_html = """
    <div style="position: relative; width: 100%; height: 90vh;">
        {map_html}
        <div style="position: absolute; top: 20px; right: 20px; background-color: rgba(255,255,255,0.95); padding: 15px; border-radius: 10px; z-index:9999; width: 300px; max-height: 80vh; overflow-y: auto; font-family: Arial;">
            <h4 style="margin-top:0;">🔥 จุดความร้อนรายจังหวัด</h4>
            <table style="width: 100%; border-collapse: collapse;">
                <thead><tr><th style="text-align:left;">จังหวัด</th><th>จำนวน</th></tr></thead>
                <tbody>
    """.format(map_html=map_html)

    for _, row in province_counts.iterrows():
        table_html += f"<tr><td>{row['ADM1_TH']}</td><td>{row['heat_spot_count']}</td></tr>"

    table_html += """
                </tbody>
            </table>
        </div>
    </div>
    """

    # Display on page
    components.html(table_html, height=800, scrolling=False)

if __name__ == "__main__":
    main()
