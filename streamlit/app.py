import streamlit as st
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

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
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        database  = "ANALYTICS_DEV",
        schema    = "MARTS",
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        role      = os.getenv("SNOWFLAKE_ROLE")
    )

@st.cache_data
def run_query(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

# ── Date filter ───────────────────────────────────────────────
st.sidebar.header("Filters")
min_date = st.sidebar.date_input("Start date", value=pd.to_datetime("2024-01-01"))
max_date = st.sidebar.date_input("End date", value=pd.to_datetime("2024-12-31"))

# ── KPI metrics row ───────────────────────────────────────────
st.subheader("Key Metrics")

kpis = run_query(f"""
    select
        count(distinct user_id)                                     as total_users,
        count(distinct order_id)                                    as total_orders,
        round(avg(total_products_in_order), 2)                      as avg_order_size,
        round(sum(total_reordered_products) * 100.0
              / nullif(sum(total_products_in_order), 0), 2)         as reorder_rate_pct
    from analytics_dev.marts.fct_orders
    where order_date between '{min_date}' and '{max_date}'
""")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Users",      f"{int(kpis['TOTAL_USERS'].iloc[0]):,}")
col2.metric("Total Orders",     f"{int(kpis['TOTAL_ORDERS'].iloc[0]):,}")
col3.metric("Avg Order Size",   f"{kpis['AVG_ORDER_SIZE'].iloc[0]}")
col4.metric("Reorder Rate",     f"{kpis['REORDER_RATE_PCT'].iloc[0]}%")

# ── DAU chart ─────────────────────────────────────────────────
st.subheader("Daily Active Users")

dau = run_query(f"""
    select
        order_date,
        count(distinct user_id) as dau
    from analytics_dev.marts.fct_orders
    where order_date between '{min_date}' and '{max_date}'
    group by order_date
    order by order_date
""")

st.line_chart(dau.set_index("ORDER_DATE"))

# ── Orders by day of week ─────────────────────────────────────
st.subheader("Orders by Day of Week")

dow = run_query(f"""
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
    where order_date between '{min_date}' and '{max_date}'
    group by order_dow, day_of_week
    order by order_dow
""")

st.bar_chart(dow.set_index("DAY_OF_WEEK")["ORDER_COUNT"])

# ── Top 10 departments by orders ──────────────────────────────
st.subheader("Top 10 Departments by Orders")

dept = run_query(f"""
    select
        p.department,
        count(op.order_id) as order_count
    from analytics_dev.marts.fct_order_products op
    join analytics_dev.marts.dim_products p
        on op.product_id = p.product_id
    join analytics_dev.marts.fct_orders o
        on op.order_id = o.order_id
    where o.order_date between '{min_date}' and '{max_date}'
    group by p.department
    order by order_count desc
    limit 10
""")

st.bar_chart(dept.set_index("DEPARTMENT")["ORDER_COUNT"])

# ── Orders by hour of day ─────────────────────────────────────
st.subheader("Orders by Hour of Day")

hourly = run_query(f"""
    select
        order_hour_of_day,
        count(order_id) as order_count
    from analytics_dev.marts.fct_orders
    where order_date between '{min_date}' and '{max_date}'
    group by order_hour_of_day
    order by order_hour_of_day
""")

st.line_chart(hourly.set_index("ORDER_HOUR_OF_DAY")["ORDER_COUNT"])