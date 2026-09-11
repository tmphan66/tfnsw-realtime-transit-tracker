import os
import boto3
import pandas as pd
import pydeck as pdk
import streamlit as st
import duckdb
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

load_dotenv()

AWS_REGION = "ap-southeast-2"
DYNAMODB_TABLE_NAME = "transit-tracker-vehicle-state"

st.set_page_config(page_title="TfNSW Sydney Real-Time Transit Tracker", layout="wide")


def fetch_live_vehicles() -> pd.DataFrame:
    table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMODB_TABLE_NAME)
    response = table.scan()
    items = response.get("Items", [])
    if not items:
        return pd.DataFrame()
    df = pd.DataFrame(items)
    for col in ["latitude", "longitude", "delay_seconds", "timestamp"]:
        df[col] = df[col].astype(float)
    if "route_short_name" not in df.columns:
        df["route_short_name"] = df["route_id"]
    df["route_short_name"] = df["route_short_name"].fillna(df["route_id"])
    return df


@st.cache_data(ttl=60)
def fetch_historical_delay_trend() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='{AWS_REGION}';
        CREATE SECRET (
            TYPE s3,
            PROVIDER credential_chain
        );
    """)
    query = f"""
        SELECT
            timestamp,
            route_id,
            delay_seconds,
            is_bunching
        FROM read_parquet('s3://{os.environ["S3_BUCKET_NAME"]}/**/*.parquet')
        ORDER BY timestamp
    """
    return con.execute(query).df()


def delay_color(delay_seconds: float, is_bunching: bool) -> list:
    if is_bunching:
        return [255, 0, 0, 200]  # red
    if delay_seconds > 300:
        return [255, 165, 0, 200]  # amber
    return [0, 200, 0, 200]  # green


st_autorefresh(interval=15000, key="refresh")

st.title("TfNSW Sydney Real-Time Transit Tracker")
st.caption(f"Last updated: {pd.Timestamp.now(tz='Australia/Sydney').strftime('%H:%M:%S')}")

df = fetch_live_vehicles()

if df.empty:
    st.warning("No live vehicle data yet. Make sure the producer and Flink job are running.")
else:
    with st.sidebar:
        st.header("Filters")
        all_agencies = sorted(df["agency_id"].unique())
        selected_agencies = st.multiselect("Agency", all_agencies, default=[], key="agency_filter")

        all_routes = sorted(df["route_short_name"].unique())
        selected_routes = st.multiselect("Route", all_routes, default=[], key="route_filter")

    if selected_agencies:
        df = df[df["agency_id"].isin(selected_agencies)]
    if selected_routes:
        df = df[df["route_short_name"].isin(selected_routes)]

    df["color"] = df.apply(lambda row: delay_color(row["delay_seconds"], row["is_bunching"]), axis=1)

    col1, col2, col3 = st.columns(3)
    col1.metric("Active vehicles", len(df))
    col2.metric("Avg delay (s)", round(df["delay_seconds"].mean(), 1))
    col3.metric("Bunching now", int(df["is_bunching"].sum()))

    legend_col1, legend_col2, legend_col3 = st.columns(3)
    legend_col1.markdown("🟢 On time")
    legend_col2.markdown("🟠 Delayed (>5 min)")
    legend_col3.markdown("🔴 Bunching")

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_fill_color="color",
        get_radius=80,
        pickable=True,
    )
    view_state = pdk.ViewState(latitude=-33.87, longitude=151.21, zoom=10)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state))

    st.dataframe(df[["vehicle_id", "agency_id", "route_short_name", "delay_seconds", "is_bunching"]].sort_values("delay_seconds", ascending=False))

    st.subheader("Historical delay trend")
    history_df = fetch_historical_delay_trend()

    if history_df.empty:
        st.info("No historical data yet. Wait 60 seconds for the first batch of data to be written to S3.")
    else:
        history_df["timestamp"] = pd.to_datetime(history_df["timestamp"], unit="s")
        trend = history_df.groupby(pd.Grouper(key="timestamp", freq="1min"))["delay_seconds"].mean().reset_index()
        st.line_chart(trend.set_index("timestamp")["delay_seconds"])