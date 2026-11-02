import os
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="NYC Taxi | Analytics",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------- Theme ----------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.stApp { background: #0b1220; color: #e8eef7; }
[data-testid="stSidebar"] { background: #101a2b; border-right: 1px solid #223149; }
.block-container { padding-top: 1.6rem; padding-bottom: 2rem; max-width: 1500px; }
h1, h2, h3 { font-family: 'Space Grotesk', sans-serif; color: #f4f7fb; }
[data-testid="stMetric"] {
  background: linear-gradient(145deg,#142238,#101a2b);
  border: 1px solid #263851; padding: 16px 18px; border-radius: 14px;
}
[data-testid="stMetricLabel"] { color: #9fb0c7; }
[data-testid="stMetricValue"] { color: #f4f7fb; }
div[data-testid="stPlotlyChart"] { background: #101a2b; border: 1px solid #223149; border-radius: 14px; padding: 8px; }
section[data-testid="stSidebar"] .stSelectbox label, section[data-testid="stSidebar"] .stDateInput label { color: #b8c6d9; }
.small-muted { color: #91a2bb; font-size: .88rem; }
.insight {
  background: #12263a; border-left: 3px solid #32c6bd; padding: 12px 15px;
  border-radius: 8px; color: #dce9f5; margin: 8px 0 18px 0;
}
hr { border-color: #263851; }
</style>
""", unsafe_allow_html=True)

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://airflow:00e74422f017edb3309ba64b1648378f@localhost:5433/taxi_warehouse"
)
engine = create_engine(DB_URL, pool_pre_ping=True)

def read_sql(query: str, params=None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params or {})

@st.cache_data(ttl=300)
def get_filter_values():
    dates = read_sql("""
        SELECT MIN(pickup_datetime)::date AS min_date,
               MAX(pickup_datetime)::date AS max_date
        FROM fact_trips
    """)
    boroughs = read_sql("""
        SELECT DISTINCT borough
        FROM dim_location
        WHERE borough IS NOT NULL
        ORDER BY borough
    """)["borough"].tolist()
    payments = read_sql("""
        SELECT payment_key, payment_description
        FROM dim_payment
        ORDER BY payment_key
    """)
    return dates.iloc[0], boroughs, payments

try:
    date_bounds, borough_values, payment_values = get_filter_values()
    if pd.isna(date_bounds["min_date"]):
        st.error("fact_trips is empty. Load the Gold data into taxi_warehouse first.")
        st.stop()
except Exception as exc:
    st.error("Could not connect to PostgreSQL. Check DATABASE_URL and make sure the database container is running.")
    st.code(str(exc))
    st.stop()

# ---------- Sidebar filters ----------
st.sidebar.markdown("## 🚕 NYC Taxi")
st.sidebar.caption("Business Intelligence")
min_date = pd.to_datetime(date_bounds["min_date"]).date()
max_date = pd.to_datetime(date_bounds["max_date"]).date()
date_range = st.sidebar.date_input(
    "Pickup date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

borough_options = ["All boroughs"] + borough_values
selected_borough = st.sidebar.selectbox("Borough", borough_options)
payment_labels = ["All payment types"] + payment_values["payment_description"].fillna("Unknown").tolist()
selected_payment = st.sidebar.selectbox("Payment type", payment_labels)

st.sidebar.markdown("---")
st.sidebar.markdown('<p class="small-muted">Dataset: NYC TLC Yellow Taxi · January 2024</p>', unsafe_allow_html=True)
st.sidebar.caption("All date filters are based on pickup date.")

where = [
    "f.pickup_datetime >= :start_ts",
    "f.pickup_datetime < (CAST(:end_date AS DATE) + INTERVAL '1 day')",
]
params = {"start_ts": start_date, "end_date": end_date}
if selected_borough != "All boroughs":
    where.append("(lp.borough = :borough OR ld.borough = :borough)")
    params["borough"] = selected_borough
if selected_payment != "All payment types":
    where.append("p.payment_description = :payment")
    params["payment"] = selected_payment
where_sql = " AND ".join(where)

base_from = """
FROM fact_trips f
LEFT JOIN dim_location lp ON f.pickup_location_key = lp.location_key
LEFT JOIN dim_location ld ON f.dropoff_location_key = ld.location_key
LEFT JOIN dim_payment p ON f.payment_key = p.payment_key
LEFT JOIN dim_vendor v ON f.vendor_key = v.vendor_key
"""

# ---------- Header ----------
st.title("NYC Taxi Analytics")
st.markdown('<p class="small-muted">A business intelligence view of January 2024 Yellow Taxi operations</p>', unsafe_allow_html=True)
st.markdown("")

kpi = read_sql(f"""
SELECT
    COUNT(*) AS trips,
    COALESCE(SUM(f.total_amount),0) AS gross_revenue,
    COALESCE(AVG(f.total_amount),0) AS avg_revenue_per_trip,
    COALESCE(AVG(f.trip_distance),0) AS avg_distance,
    COALESCE(AVG(EXTRACT(EPOCH FROM (f.dropoff_datetime-f.pickup_datetime))/60),0) AS avg_duration,
    COALESCE(AVG(f.tip_amount),0) AS avg_tip
{base_from}
WHERE {where_sql}
""", params).iloc[0]

c1,c2,c3,c4,c5,c6 = st.columns(6)
c1.metric("Trips", f"{int(kpi.trips):,}")
c2.metric("Gross revenue", f"${kpi.gross_revenue:,.0f}")
c3.metric("Revenue / trip", f"${kpi.avg_revenue_per_trip:,.2f}")
c4.metric("Avg. distance", f"{kpi.avg_distance:,.2f} mi")
c5.metric("Avg. duration", f"{kpi.avg_duration:,.1f} min")
c6.metric("Avg. tip", f"${kpi.avg_tip:,.2f}")

# ---------- Executive overview ----------
st.markdown("## Executive overview")
daily = read_sql(f"""
SELECT f.pickup_datetime::date AS trip_date,
       COUNT(*) AS trips,
       SUM(f.total_amount) AS revenue
{base_from}
WHERE {where_sql}
GROUP BY 1 ORDER BY 1
""", params)
left,right = st.columns([1.35,1])
with left:
    fig = go.Figure()
    fig.add_trace(go.Bar(x=daily.trip_date, y=daily.trips, name="Trips", marker_color="#32c6bd", yaxis="y"))
    fig.add_trace(go.Scatter(x=daily.trip_date, y=daily.revenue, name="Revenue", line=dict(color="#f6b85e",width=3), yaxis="y2"))
    fig.update_layout(
        title="Daily trip volume & revenue", template="plotly_dark", height=360,
        paper_bgcolor="#101a2b", plot_bgcolor="#101a2b",
        yaxis=dict(title="Trips", gridcolor="#263851"),
        yaxis2=dict(title="Revenue ($)", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.12), margin=dict(l=20,r=20,t=65,b=20)
    )
    st.plotly_chart(fig, use_container_width=True)
with right:
    dow = read_sql(f"""
    SELECT TO_CHAR(f.pickup_datetime,'Dy') AS weekday,
           EXTRACT(ISODOW FROM f.pickup_datetime) AS weekday_num,
           COUNT(*) AS trips, SUM(f.total_amount) AS revenue
    {base_from}
    WHERE {where_sql}
    GROUP BY 1,2 ORDER BY 2
    """, params)
    fig = px.bar(dow, x="weekday", y="trips", color="revenue",
                 color_continuous_scale=["#245b70","#32c6bd","#f6b85e"],
                 title="Demand by weekday", template="plotly_dark")
    fig.update_layout(height=360, paper_bgcolor="#101a2b", plot_bgcolor="#101a2b",
                      coloraxis_colorbar_title="Revenue ($)", margin=dict(l=20,r=20,t=65,b=20))
    st.plotly_chart(fig, use_container_width=True)

if len(daily):
    best_day = daily.loc[daily.revenue.idxmax()]
    st.markdown(
        f'<div class="insight">Highest-revenue pickup date in the selected period: '
        f'<b>{best_day.trip_date}</b> with <b>${best_day.revenue:,.0f}</b> across '
        f'<b>{int(best_day.trips):,}</b> trips.</div>', unsafe_allow_html=True
    )

# ---------- Demand patterns ----------
st.markdown("## Demand patterns")
hourly = read_sql(f"""
SELECT EXTRACT(HOUR FROM f.pickup_datetime)::int AS hour,
       EXTRACT(ISODOW FROM f.pickup_datetime)::int AS weekday_num,
       TO_CHAR(f.pickup_datetime,'Dy') AS weekday,
       COUNT(*) AS trips
{base_from}
WHERE {where_sql}
GROUP BY 1,2,3
ORDER BY 2,1
""", params)
heat = hourly.pivot(index="weekday", columns="hour", values="trips").fillna(0)
heat = heat.reindex(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"])
fig = px.imshow(heat, aspect="auto", color_continuous_scale=["#142238","#176b78","#32c6bd","#f6b85e"],
                labels=dict(x="Pickup hour", y="Weekday", color="Trips"),
                title="Pickup demand heatmap · weekday × hour", template="plotly_dark")
fig.update_layout(height=330, paper_bgcolor="#101a2b", margin=dict(l=20,r=20,t=55,b=20))
st.plotly_chart(fig, use_container_width=True)

# ---------- Geography ----------
st.markdown("## Geographic intelligence")
g1,g2 = st.columns(2)
pickup = read_sql(f"""
SELECT lp.borough, lp.zone, COUNT(*) AS trips, SUM(f.total_amount) AS revenue,
       AVG(f.total_amount) AS avg_trip_amount
{base_from}
WHERE {where_sql} AND lp.zone IS NOT NULL
GROUP BY 1,2 ORDER BY trips DESC LIMIT 15
""", params)
dropoff = read_sql(f"""
SELECT ld.borough, ld.zone, COUNT(*) AS trips, SUM(f.total_amount) AS revenue
{base_from}
WHERE {where_sql} AND ld.zone IS NOT NULL
GROUP BY 1,2 ORDER BY trips DESC LIMIT 15
""", params)
with g1:
    fig = px.bar(pickup.sort_values("trips"), x="trips", y="zone", color="borough",
                 orientation="h", title="Top 15 pickup zones", template="plotly_dark",
                 color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(height=500, paper_bgcolor="#101a2b", plot_bgcolor="#101a2b",
                      margin=dict(l=10,r=10,t=55,b=20), yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
with g2:
    fig = px.bar(dropoff.sort_values("trips"), x="trips", y="zone", color="borough",
                 orientation="h", title="Top 15 dropoff zones", template="plotly_dark",
                 color_discrete_sequence=px.colors.qualitative.Pastel)
    fig.update_layout(height=500, paper_bgcolor="#101a2b", plot_bgcolor="#101a2b",
                      margin=dict(l=10,r=10,t=55,b=20), yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

od = read_sql(f"""
SELECT lp.zone AS pickup_zone, ld.zone AS dropoff_zone, COUNT(*) AS trips
{base_from}
WHERE {where_sql} AND lp.zone IS NOT NULL AND ld.zone IS NOT NULL
GROUP BY 1,2
ORDER BY trips DESC LIMIT 20
""", params)
st.markdown("### Most common pickup → dropoff routes")
st.dataframe(od, use_container_width=True, hide_index=True)

# ---------- Revenue & payment ----------
st.markdown("## Revenue & payment")
rev1,rev2 = st.columns(2)
components = read_sql(f"""
SELECT SUM(f.fare_amount) AS fare,
       SUM(f.extra) AS extra,
       SUM(f.mta_tax) AS mta_tax,
       SUM(f.tip_amount) AS tips,
       SUM(f.tolls_amount) AS tolls,
       SUM(f.improvement_surcharge) AS improvement_surcharge,
       SUM(f.congestion_surcharge) AS congestion_surcharge,
       SUM(f.airport_fee) AS airport_fee
{base_from}
WHERE {where_sql}
""", params).melt(var_name="component", value_name="amount")
with rev1:
    fig = px.bar(components.sort_values("amount"), x="amount", y="component",
                 orientation="h", title="Revenue components", template="plotly_dark",
                 color="amount", color_continuous_scale=["#245b70","#32c6bd","#f6b85e"])
    fig.update_layout(height=390, paper_bgcolor="#101a2b", plot_bgcolor="#101a2b",
                      margin=dict(l=10,r=10,t=55,b=20), coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)
payment = read_sql(f"""
SELECT COALESCE(p.payment_description,'Unknown') AS payment_method,
       COUNT(*) AS trips, SUM(f.total_amount) AS revenue,
       AVG(f.tip_amount) AS avg_tip
{base_from}
WHERE {where_sql}
GROUP BY 1 ORDER BY trips DESC
""", params)
with rev2:
    fig = px.pie(payment, names="payment_method", values="trips", hole=.58,
                 title="Trips by payment method", template="plotly_dark",
                 color_discrete_sequence=["#32c6bd","#f6b85e","#5d8bd8","#b48bd6","#e17c65"])
    fig.update_layout(height=390, paper_bgcolor="#101a2b", margin=dict(l=10,r=10,t=55,b=20))
    st.plotly_chart(fig, use_container_width=True)
st.dataframe(payment, use_container_width=True, hide_index=True)

# ---------- Trip behaviour ----------
st.markdown("## Trip behaviour & performance")
behavior = read_sql(f"""
SELECT f.trip_distance,
       EXTRACT(EPOCH FROM (f.dropoff_datetime-f.pickup_datetime))/60.0 AS duration_min,
       f.fare_amount, f.tip_amount, f.total_amount,
       f.passenger_count,
       COALESCE(p.payment_description,'Unknown') AS payment_method
{base_from}
WHERE {where_sql}
  AND f.trip_distance > 0
  AND f.dropoff_datetime > f.pickup_datetime
  AND EXTRACT(EPOCH FROM (f.dropoff_datetime-f.pickup_datetime)) BETWEEN 60 AND 21600
  AND f.trip_distance <= 100
  AND f.total_amount BETWEEN 0 AND 1000
""", params)
b1,b2 = st.columns(2)
with b1:
    fig = px.histogram(behavior, x="duration_min", nbins=60,
                       title="Trip duration distribution · filtered valid range",
                       template="plotly_dark", color_discrete_sequence=["#32c6bd"])
    fig.update_layout(height=350, paper_bgcolor="#101a2b", plot_bgcolor="#101a2b",
                      xaxis_title="Duration (minutes)", yaxis_title="Trips",
                      margin=dict(l=10,r=10,t=55,b=20))
    st.plotly_chart(fig, use_container_width=True)
with b2:
    sample = behavior.sample(min(len(behavior), 12000), random_state=42) if len(behavior) else behavior
    fig = px.scatter(sample, x="trip_distance", y="fare_amount", color="payment_method",
                     opacity=.45, title="Fare vs. trip distance · sample of up to 12k trips",
                     template="plotly_dark", render_mode="webgl")
    fig.update_layout(height=350, paper_bgcolor="#101a2b", plot_bgcolor="#101a2b",
                      xaxis_title="Distance (miles)", yaxis_title="Fare ($)",
                      margin=dict(l=10,r=10,t=55,b=20))
    st.plotly_chart(fig, use_container_width=True)

# ---------- Vendors ----------
st.markdown("## Vendor comparison")
vendors = read_sql(f"""
SELECT COALESCE(v.vendor_name,'Unknown') AS vendor,
       COUNT(*) AS trips,
       SUM(f.total_amount) AS revenue,
       AVG(f.total_amount) AS avg_total,
       AVG(f.trip_distance) AS avg_distance,
       AVG(f.tip_amount) AS avg_tip
{base_from}
WHERE {where_sql}
GROUP BY 1 ORDER BY revenue DESC
""", params)
st.dataframe(vendors, use_container_width=True, hide_index=True)

st.markdown("---")
st.caption("Source: NYC TLC Yellow Taxi Trip Records · January 2024. Metrics are calculated from the selected pickup-date range. Geographic results use TLC zone labels; no coordinates are inferred.")
