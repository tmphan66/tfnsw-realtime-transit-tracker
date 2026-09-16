import math
import os

import altair as alt
import boto3
import duckdb
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = "ap-southeast-2"
DYNAMODB_TABLE_NAME = "transit-tracker-vehicle-state"
DBT_DUCKDB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dbt", "transit_analytics.duckdb")

SYDNEY_METRO_VIEW = pdk.ViewState(latitude=-33.87, longitude=151.15, zoom=10.5)

TOOLTIP_CONFIG = {
    "html": """
        <b>Vehicle:</b> {vehicle_id}<br/>
        <b>Route:</b> {route_short_name}<br/>
        <b>Operator:</b> {operator_name}<br/>
        <b>Delay:</b> {delay_seconds}s<br/>
        <b>Bunching:</b> {is_bunching_label}
    """,
    "style": {
        "backgroundColor": "#1e1e1e",
        "color": "white",
    },
}

st.set_page_config(page_title="TfNSW Sydney Real-Time Transit Tracker", layout="wide")


@st.cache_data(ttl=15)
def fetch_live_vehicles() -> pd.DataFrame:
    table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMODB_TABLE_NAME)
    response = table.scan(
        ProjectionExpression="vehicle_id, route_id, agency_id, route_short_name, latitude, longitude, delay_seconds, is_bunching, #ts",
        ExpressionAttributeNames={"#ts": "timestamp"},
    )
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


def filter_valid_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """TfNSW reports (0,0) for vehicles without a GPS fix yet — filter this
    placeholder out before it distorts any map/zoom calculation."""
    return df[~((df["latitude"] == 0) & (df["longitude"] == 0))]


def build_recent_silver_paths(bucket: str, days_back: int = 2) -> list:
    # Must match Flink's partitioning, which uses UTC (datetime.now(timezone.utc)) —
    # using local (Sydney) time here would look in the wrong-dated folder for a
    # large chunk of every day, since Sydney is UTC+10/+11.
    now = pd.Timestamp.now(tz="UTC")
    paths = []
    for i in range(days_back):
        d = now - pd.Timedelta(days=i)
        paths.append(f"s3://{bucket}/silver/vehicle-events/year={d.year}/month={d.month:02d}/day={d.day:02d}/*.parquet")
    return paths


@st.cache_data(ttl=60)
def fetch_historical_delay_trend(days_back: int = 2) -> pd.DataFrame:
    """Tries the recent window first (fast, bounded cost regardless of how
    long the pipeline has been running). Falls back to a full bucket scan
    only if the recent window is empty - e.g. right after a fresh S3 flush
    or on first load before today's/yesterday's partitions exist yet."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='{AWS_REGION}';
        CREATE SECRET (
            TYPE s3,
            PROVIDER credential_chain
        );
    """)

    bucket = os.environ["S3_BUCKET_NAME"]

    def run_query(source_sql: str) -> pd.DataFrame:
        query = f"""
            SELECT
                timestamp,
                route_id,
                agency_id,
                delay_seconds,
                is_bunching
            FROM read_parquet({source_sql}, union_by_name=true)
            ORDER BY timestamp
        """
        return con.execute(query).df()

    recent_paths = build_recent_silver_paths(bucket, days_back=days_back)
    recent_path_list_sql = "[" + ", ".join(f"'{p}'" for p in recent_paths) + "]"

    try:
        result = run_query(recent_path_list_sql)
        if not result.empty:
            return result
    except duckdb.IOException:
        pass

    # Fallback: recent window had nothing — scan the whole bucket instead.
    full_scan_sql = f"'s3://{bucket}/silver/vehicle-events/**/*.parquet'"
    try:
        return run_query(full_scan_sql)
    except duckdb.IOException:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def fetch_gold_table(table_name: str) -> pd.DataFrame:
    con = duckdb.connect(DBT_DUCKDB_PATH, read_only=True)
    return con.execute(f"SELECT * FROM {table_name}").df()


@st.cache_data(ttl=300)
def fetch_agency_lookup() -> dict:
    lookup_df = fetch_gold_table("agency_lookup")
    return dict(zip(lookup_df["agency_id"], lookup_df["agency_name"]))


def add_operator_name(df: pd.DataFrame, agency_lookup: dict) -> pd.DataFrame:
    df = df.copy()
    df["operator_name"] = df["agency_id"].map(agency_lookup).fillna(
        "Unknown operator (" + df["agency_id"].astype(str) + ")"
    )
    return df


def add_delay_color(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized instead of row-wise .apply() — meaningfully faster as
    the vehicle count grows. Colors: red=bunching, amber=delayed>5min, green=on time."""
    df = df.copy()
    conditions = [df["is_bunching"], df["delay_seconds"] > 300]

    r = np.select(conditions, [255, 255], default=0)
    g = np.select(conditions, [0, 165], default=200)
    b = np.select(conditions, [0, 0], default=0)
    a = np.full(len(df), 200)

    df["color"] = list(zip(r.tolist(), g.tolist(), b.tolist(), a.tolist()))
    df["color"] = df["color"].apply(list)
    return df


def compute_view_state(df: pd.DataFrame, padding_factor: float = 1.3, min_spread: float = 0.005) -> pdk.ViewState:
    if df.empty:
        return pdk.ViewState(latitude=-33.87, longitude=151.21, zoom=9)

    min_lat, max_lat = df["latitude"].min(), df["latitude"].max()
    min_lon, max_lon = df["longitude"].min(), df["longitude"].max()
    center_lat = (min_lat + max_lat) / 2
    center_lon = (min_lon + max_lon) / 2

    lat_spread = max(max_lat - min_lat, min_spread) * padding_factor
    lon_spread = max(max_lon - min_lon, min_spread) * padding_factor

    lat_zoom = math.log2(180 / lat_spread)
    lon_zoom = math.log2(360 / lon_spread)
    zoom = max(2, min(min(lat_zoom, lon_zoom), 16))

    return pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=zoom)


def truncate_label(name: str, max_len: int = 18) -> str:
    return name if len(name) <= max_len else name[: max_len - 1] + "…"


def make_single_metric_bar_chart(df: pd.DataFrame, category_col: str, value_col: str, y_title: str, color: str, floor_at_zero: bool = True):
    plot_df = df.copy()
    plot_df["_axis_label"] = plot_df[category_col].apply(truncate_label)

    y_scale = alt.Scale(domainMin=0) if floor_at_zero else alt.Scale()

    chart = alt.Chart(plot_df).mark_bar(color=color).encode(
        x=alt.X("_axis_label:N", title=None, sort=None,
                axis=alt.Axis(labelAngle=-45, labelPadding=8, labelOverlap=False)),
        y=alt.Y(f"{value_col}:Q", title=y_title, scale=y_scale),
        tooltip=[category_col, value_col],
    ).properties(height=320)
    return chart


def make_comparison_bar_chart(selection_value: float, network_value: float, y_title: str, floor_at_zero: bool = True):
    compare_df = pd.DataFrame({
        "group": ["Selection", "Network average"],
        "value": [selection_value, network_value],
    })
    y_scale = alt.Scale(domainMin=0) if floor_at_zero else alt.Scale()
    chart = alt.Chart(compare_df).mark_bar().encode(
        x=alt.X("group:N", title=None, sort=None),
        y=alt.Y("value:Q", title=y_title, scale=y_scale),
        color=alt.Color("group:N", title=None, scale=alt.Scale(
            domain=["Selection", "Network average"],
            range=["#4C9BE8", "#8A8A8A"],
        )),
        tooltip=["group", "value"],
    ).properties(height=300)
    return chart


def make_delay_trend_chart(history_df: pd.DataFrame, color: str = "#4C9BE8"):
    trend = (
        history_df.groupby(pd.Grouper(key="timestamp", freq="1min"))["delay_seconds"]
        .mean()
        .reset_index()
    )
    return alt.Chart(trend).mark_line(color=color).encode(
        x=alt.X("timestamp:T", title="Time", axis=alt.Axis(format="%H:%M", labelAngle=0)),
        y=alt.Y("delay_seconds:Q", title="Average delay (seconds)"),
        tooltip=["timestamp:T", "delay_seconds:Q"],
    )


@st.fragment(run_every="15s")
def render_dashboard():
    st.caption(f"Last updated: {pd.Timestamp.now(tz='Australia/Sydney').strftime('%H:%M:%S')}")

    df = fetch_live_vehicles()

    if df.empty:
        st.warning("No live vehicle data yet. Make sure the producer and Flink job are running.")
        return

    df = filter_valid_coordinates(df)
    agency_lookup = fetch_agency_lookup()
    df = add_operator_name(df, agency_lookup)
    df["is_bunching_label"] = df["is_bunching"].map({True: "Yes", False: "No"})

    history_df = fetch_historical_delay_trend()
    if not history_df.empty:
        history_df["timestamp"] = (
            pd.to_datetime(history_df["timestamp"], unit="s", utc=True)
            .dt.tz_convert("Australia/Sydney")
        )

    tab_overview, tab_deep_dive = st.tabs(["Network Overview", "Route / Operator View"])

    # ---------------- TAB 1: NETWORK OVERVIEW (unfiltered) ----------------
    with tab_overview:
        df_o = add_delay_color(df)

        col1, col2, col3 = st.columns(3)
        col1.metric("Active vehicles", len(df_o))
        col2.metric("Avg delay (s)", round(df_o["delay_seconds"].mean(), 1))
        col3.metric("Bunching now", int(df_o["is_bunching"].sum()))

        legend_col1, legend_col2, legend_col3 = st.columns(3)
        legend_col1.markdown("🟢 On time")
        legend_col2.markdown("🟠 Delayed (>5 min)")
        legend_col3.markdown("🔴 Bunching")

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_o,
            get_position=["longitude", "latitude"],
            get_fill_color="color",
            get_radius=80,
            pickable=True,
        )
        st.pydeck_chart(pdk.Deck(
            layers=[layer],
            initial_view_state=SYDNEY_METRO_VIEW,
            tooltip=TOOLTIP_CONFIG,
        ))

        st.dataframe(
            df_o[["vehicle_id", "operator_name", "route_short_name", "delay_seconds", "is_bunching"]]
            .sort_values("delay_seconds", ascending=False),
            hide_index=True,
        )

        st.subheader("Historical delay trend (network-wide)")
        if history_df.empty:
            st.info("No historical data yet. Wait 60 seconds for the first batch of data to be written to S3.")
        else:
            st.altair_chart(make_delay_trend_chart(history_df), width="stretch")

        st.subheader("Overview: Is delay explained by volume?")
        try:
            busiest_routes_df = fetch_gold_table("busiest_routes")
            operator_df = fetch_gold_table("operator_performance")
            top_routes_df = fetch_gold_table("top_routes_by_delay")

            top_15_routes = busiest_routes_df.sort_values("active_vehicle_count", ascending=False).head(15)
            top_15_operators = operator_df.sort_values("active_vehicle_count", ascending=False).head(15)

            st.markdown("**Busiest routes**")
            st.dataframe(top_15_routes, hide_index=True)
            r_col1, r_col2 = st.columns(2)
            with r_col1:
                st.altair_chart(
                    make_single_metric_bar_chart(top_15_routes, "route_short_name", "active_vehicle_count", "Active vehicles", "#4C9BE8"),
                    width="stretch",
                )
            with r_col2:
                st.altair_chart(
                    make_single_metric_bar_chart(top_15_routes, "route_short_name", "average_delay_seconds", "Average delay (seconds)", "#E88A4C", floor_at_zero=False),
                    width="stretch",
                )

            st.markdown("**Operator performance**")
            st.dataframe(top_15_operators, hide_index=True)
            op_col1, op_col2 = st.columns(2)
            with op_col1:
                st.altair_chart(
                    make_single_metric_bar_chart(top_15_operators, "operator_name", "active_vehicle_count", "Active vehicles", "#4C9BE8"),
                    width="stretch",
                )
            with op_col2:
                st.altair_chart(
                    make_single_metric_bar_chart(top_15_operators, "operator_name", "average_delay_seconds", "Average delay (seconds)", "#E88A4C", floor_at_zero=False),
                    width="stretch",
                )

            st.markdown("**Bunching events vs. volume**")
            st.caption("Same order as above (busiest first) — compare against the volume bars for proportionality")
            b_col1, b_col2 = st.columns(2)
            with b_col1:
                st.altair_chart(
                    make_single_metric_bar_chart(top_15_routes, "route_short_name", "bunching_events", "Bunching events", "#4C9BE8"),
                    width="stretch",
                )
            with b_col2:
                st.altair_chart(
                    make_single_metric_bar_chart(top_15_operators, "operator_name", "bunching_events", "Bunching events", "#E88A4C"),
                    width="stretch",
                )

            st.markdown("**Best & worst 5 routes by average delay** (minimum sample size applied)")
            st.dataframe(top_routes_df, hide_index=True)

        except duckdb.CatalogException:
            st.info("Gold-layer tables not built yet — run `dbt run` in the dbt/ folder first.")

    # ---------------- TAB 2: ROUTE / OPERATOR DEEP-DIVE ----------------
    with tab_deep_dive:
        st.markdown("View by **either** a specific route or a specific operator.")

        view_mode = st.radio("View by", ["Route", "Operator"], horizontal=True, key="dd_mode")

        df_d = df.copy()
        selection_label = None
        selected_route_id = None
        selected_agency_ids = None

        if view_mode == "Route":
            route_lookup = (
                df[["route_id", "route_short_name", "operator_name"]]
                .drop_duplicates()
                .sort_values(["route_short_name", "operator_name"])
            )
            route_lookup["display_label"] = route_lookup["route_short_name"] + " (" + route_lookup["operator_name"] + ")"

            selected_label = st.selectbox("Route", route_lookup["display_label"].tolist(), key="dd_route")
            selected_route_id = route_lookup.loc[route_lookup["display_label"] == selected_label, "route_id"].iloc[0]

            df_d = df_d[df_d["route_id"] == selected_route_id]
            selection_label = selected_label
        else:
            operator_options = sorted(df["operator_name"].unique())
            selected_operator = st.selectbox("Operator", operator_options, key="dd_operator")
            df_d = df_d[df_d["operator_name"] == selected_operator]
            selected_agency_ids = df_d["agency_id"].unique().tolist()
            selection_label = selected_operator

        if df_d.empty:
            st.warning("No active vehicles currently match this selection.")
        else:
            df_d = add_delay_color(df_d)

            network_avg_delay = df["delay_seconds"].mean()
            selection_avg_delay = df_d["delay_seconds"].mean()
            network_bunching_count = df["is_bunching"].sum()
            selection_bunching_count = df_d["is_bunching"].sum()
            network_bunching_rate = df["is_bunching"].mean() * 100
            selection_bunching_rate = df_d["is_bunching"].mean() * 100

            st.subheader(selection_label)

            m_col1, m_col2, m_col3 = st.columns(3)
            m_col1.metric("Active vehicles (selection)", len(df_d))
            m_col2.metric(
                "Avg delay (selection)",
                f"{selection_avg_delay:.0f}s",
                delta=f"{selection_avg_delay - network_avg_delay:+.0f}s vs network avg",
                delta_color="inverse",
            )
            m_col3.metric(
                "Bunching rate (selection)",
                f"{selection_bunching_rate:.0f}%",
                delta=f"{selection_bunching_rate - network_bunching_rate:+.0f}pp vs network avg",
                delta_color="inverse",
            )

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=df_d,
                get_position=["longitude", "latitude"],
                get_fill_color="color",
                get_radius=80,
                pickable=True,
            )
            st.pydeck_chart(pdk.Deck(
                layers=[layer],
                initial_view_state=compute_view_state(df_d, padding_factor=1.1, min_spread=0.003),
                tooltip=TOOLTIP_CONFIG,
            ))

            st.dataframe(
                df_d[["vehicle_id", "operator_name", "route_short_name", "delay_seconds", "is_bunching"]]
                .sort_values("delay_seconds", ascending=False),
                hide_index=True,
            )

            st.markdown("**Selection vs. network average**")
            chart_col1, chart_col2 = st.columns(2)
            with chart_col1:
                st.altair_chart(
                    make_comparison_bar_chart(selection_avg_delay, network_avg_delay, "Average delay (seconds)", floor_at_zero=False),
                    width="stretch",
                )
            with chart_col2:
                st.altair_chart(
                    make_comparison_bar_chart(selection_bunching_count, network_bunching_count, "Bunching event count"),
                    width="stretch",
                )

            st.markdown(f"**Delay trend for {selection_label}**")
            if history_df.empty:
                st.info("No historical data yet for this selection.")
            else:
                if view_mode == "Route":
                    selection_history_df = history_df[history_df["route_id"] == selected_route_id]
                else:
                    selection_history_df = history_df[history_df["agency_id"].isin(selected_agency_ids)]

                if selection_history_df.empty:
                    st.info("No historical data yet for this selection.")
                else:
                    st.altair_chart(
                        make_delay_trend_chart(selection_history_df, color="#8A63D2"),
                        width="stretch",
                    )


st.title("TfNSW Sydney Real-Time Transit Tracker")
render_dashboard()