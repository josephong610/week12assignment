# dashboard.py
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://kafka_user:kafka_password@localhost:5432/kafka_db"

st.set_page_config(page_title="Real-Time Ride-Sharing Dashboard", page_icon="🚕", layout="wide")


@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def load_data(engine, limit: int = 500):
    query = text(
        """
        SELECT
            trip_id,
            driver_id,
            rider_id,
            pickup_area,
            dropoff_area,
            pickup_time,
            dropoff_time,
            distance_km,
            fare,
            surge_multiplier,
            status,
            payment_method,
            created_at
        FROM rides
        ORDER BY pickup_time DESC NULLS LAST, created_at DESC
        LIMIT :limit;
        """
    )
    with engine.begin() as conn:
        df = pd.read_sql_query(query, conn, params={"limit": limit})
    if not df.empty:
        for col in ["pickup_time", "dropoff_time", "created_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
    return df


def main():

    st.title("🚕 Real-Time Ride-Sharing Dashboard")

    # --------------------------------------------
    # Sidebar controls
    # --------------------------------------------
    st.sidebar.header("Controls")
    limit = st.sidebar.slider("Number of recent rides", 50, 1000, 300, 50)
    update_interval = st.sidebar.slider("Refresh every (seconds)", 2, 30, 5, 1)

    st.sidebar.markdown("### How to use")
    st.sidebar.markdown(
        "- Run `docker compose up`.\n"
        "- Run `python producer.py`.\n"
        "- Run `python consumer.py`.\n"
        "- Then run this dashboard via `streamlit run dashboard.py`."
    )

    engine = get_engine()
    df = load_data(engine, limit=limit)

    # --------------------------------------------
    # No data case
    # --------------------------------------------
    if df.empty:
        st.info("No rides yet. Start the producer and consumer.")
        # show message, then refresh
        time.sleep(update_interval)
        st.rerun()
        return

    # --------------------------------------------
    # Metrics
    # --------------------------------------------
    total_rides = len(df)
    completed = (df["status"] == "completed").sum()
    avg_fare = df["fare"].mean()
    avg_distance = df["distance_km"].mean()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total recent rides", f"{total_rides}")
    col2.metric("Completed rides", f"{completed}")
    col3.metric("Average fare", f"${avg_fare:0.2f}")
    col4.metric("Avg distance (km)", f"{avg_distance:0.2f}")

    # --------------------------------------------
    # Table
    # --------------------------------------------
    st.markdown("### Recent rides")
    st.dataframe(
        df[
            [
                "trip_id",
                "pickup_time",
                "pickup_area",
                "dropoff_area",
                "distance_km",
                "fare",
                "surge_multiplier",
                "status",
            ]
        ],
        use_container_width=True,
    )

    # --------------------------------------------
    # Rides per minute chart
    # --------------------------------------------
    if "pickup_time" in df.columns:
        ts = (
            df.assign(pickup_minute=df["pickup_time"].dt.floor("min"))
            .groupby("pickup_minute")
            .size()
            .reset_index(name="rides")
        )

        fig_ts = px.line(
            ts,
            x="pickup_minute",
            y="rides",
            title="Rides per minute (recent window)",
        )
        st.plotly_chart(fig_ts, use_container_width=True)

    # --------------------------------------------
    # Fare by pickup/dropoff
    # --------------------------------------------
    st.markdown("### Average fare by area")
    col_a, col_b = st.columns(2)

    with col_a:
        df_pickup = (
            df.groupby("pickup_area")["fare"]
            .mean()
            .reset_index()
            .sort_values("fare", ascending=False)
            .head(15)
        )
        fig_pickup = px.bar(df_pickup, x="pickup_area", y="fare", title="Avg fare by pickup area")
        st.plotly_chart(fig_pickup, use_container_width=True)

    with col_b:
        df_dropoff = (
            df.groupby("dropoff_area")["fare"]
            .mean()
            .reset_index()
            .sort_values("fare", ascending=False)
            .head(15)
        )
        fig_dropoff = px.bar(df_dropoff, x="dropoff_area", y="fare", title="Avg fare by dropoff area")
        st.plotly_chart(fig_dropoff, use_container_width=True)

    st.caption(f"Last updated: {datetime.utcnow().isoformat()} • Auto-refreshing every {update_interval}s")

    # --------------------------------------------
    # NOW refresh — at the END
    # --------------------------------------------
    time.sleep(update_interval)
    st.rerun()


if __name__ == "__main__":
    main()
