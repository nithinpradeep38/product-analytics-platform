import streamlit as st
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# ── Page config ───────────────────────────────────────────────
st.set_page_config(
    page_title="Instacart Product Analytics",
    page_icon="🛒",
    layout="wide"
)

st.title("🛒 Instacart Product Analytics Platform")
st.markdown("End-to-end analytics engineering project · Snowflake · dbt · MetricFlow")

# ── Connection ────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="ANALYTICS_DEV",
        schema="MARTS",
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

# ── Safe query runner ─────────────────────────────────────────
@st.cache_data(ttl=600)
def run_query(query, params=None):
    conn = get_connection()
    return pd.read_sql(query, conn, params=params)

# ── Sidebar filters ───────────────────────────────────────────
st.sidebar.header("Filters")

min_date, max_date = st.sidebar.date_input(
    "Select date range",
    value=(pd.to_datetime("2024-01-01").date(),
           pd.to_datetime("2024-12-31").date())
)

# Normalize (CRITICAL FIX)
min_date = pd.to_datetime(min_date).date()
max_date = pd.to_datetime(max_date).date()

# ── Guard clause ──────────────────────────────────────────────
if not min_date or not max_date:
    st.stop()

# ── KPI QUERY ─────────────────────────────────────────────────
st.subheader("Key Metrics")

kpis_query = """
select
    count(distinct user_id) as total_users,
    count(distinct order_id) as total_orders,
    round(avg(total_products_in_order), 2) as avg_order_size,
    round(
        sum(total_reordered_products) * 100.0 /
        nullif(sum(total_products_in_order), 0),
        2
    ) as reorder_rate_pct
from analytics_dev.marts.fct_orders
where order_date::date between %s and %s
"""

kpis = run_query(kpis_query, [min_date, max_date])

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Users", f"{int(kpis['TOTAL_USERS'][0]):,}")
col2.metric("Total Orders", f"{int(kpis['TOTAL_ORDERS'][0]):,}")
col3.metric("Avg Order Size", f"{kpis['AVG_ORDER_SIZE'][0]}")
col4.metric("Reorder Rate", f"{kpis['REORDER_RATE_PCT'][0]}%")

# ── DAU ───────────────────────────────────────────────────────
st.subheader("Daily Active Users")

dau_query = """
select
    order_date::date as order_date,
    count(distinct user_id) as dau
from analytics_dev.marts.fct_orders
where order_date::date between %s and %s
group by 1
order by 1
"""

dau = run_query(dau_query, [min_date, max_date])

st.line_chart(dau.set_index("ORDER_DATE"))

# ── Orders by Day of Week ─────────────────────────────────────
st.subheader("Orders by Day of Week")

dow_query = """
select
    case order_dow
        when 0 then 'Sunday'
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
    end as day_of_week,
    order_dow,
    count(order_id) as order_count
from analytics_dev.marts.fct_orders
where order_date::date between %s and %s
group by 1, 2
order by 2
"""

dow = run_query(dow_query, [min_date, max_date])

st.bar_chart(dow.set_index("DAY_OF_WEEK")["ORDER_COUNT"])

# ── Top Departments ───────────────────────────────────────────
st.subheader("Top 10 Departments by Orders")

dept_query = """
select
    p.department,
    count(op.order_id) as order_count
from analytics_dev.marts.fct_order_products op
join analytics_dev.marts.dim_products p
    on op.product_id = p.product_id
join analytics_dev.marts.fct_orders o
    on op.order_id = o.order_id
where o.order_date::date between %s and %s
group by 1
order by 2 desc
limit 10
"""

dept = run_query(dept_query, [min_date, max_date])

st.bar_chart(dept.set_index("DEPARTMENT")["ORDER_COUNT"])

# ── Orders by Hour ────────────────────────────────────────────
st.subheader("Orders by Hour of Day")

hour_query = """
select
    order_hour_of_day,
    count(order_id) as order_count
from analytics_dev.marts.fct_orders
where order_date::date between %s and %s
group by 1
order by 1
"""

hourly = run_query(hour_query, [min_date, max_date])

st.line_chart(hourly.set_index("ORDER_HOUR_OF_DAY")["ORDER_COUNT"])