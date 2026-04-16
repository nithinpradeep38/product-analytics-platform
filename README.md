# 🛒 Product Analytics Platform

An end-to-end analytics engineering project built at the intersection of 
**product analytics** and **analytics engineering**.

## 🚀 Live Demo
**[View the Streamlit Dashboard](https://instacartanalytics.streamlit.app/)**  
**[View the dbt Docs](https://nithinpradeep38.github.io/product-analytics-platform/)**


## 🧰 Tech Stack

- **Data Warehouse:** Snowflake  
- **Transformation:** dbt Core (3-layer architecture: staging → intermediate → marts)  
- **Semantic Layer / Metrics:** MetricFlow (dbt Semantic Layer)  
- **Orchestration:** Airflow  
- **Analytics Engineering:** Python (pandas)  
- **Dashboarding:** Streamlit  
- **CI/CD:** GitHub Actions  
- **Dataset:** Instacart Market Basket Analysis (Kaggle)

---

## 📊 Project Overview

This project demonstrates a production-grade analytics stack built on the 
Instacart Online Grocery Shopping Dataset (3M+ orders, 206K+ users).

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + pandas | Load raw CSVs into Snowflake with synthetic date generation |
| Warehouse | Snowflake | Cloud data warehouse |
| Transformation | dbt Core 1.9 | 3-layer modelling (staging → intermediate → marts) |
| Metrics | dbt MetricFlow | Centralised, versioned metric definitions |
| Orchestration | Airflow | Pipeline scheduling (ingest → dbt run → dbt test) |
| Dashboard | Streamlit | Live product analytics dashboard |
| CI/CD | GitHub Actions | Automated dbt build + test on every PR |

---

## 🏗️ Architecture

![Schema](docs/schema.png)

### dbt Layer Structure
```text
models/
├── staging/          # 1:1 with raw sources, light cleaning only
│   ├── stg_orders
│   ├── stg_order_products
│   ├── stg_products
│   ├── stg_aisles
│   └── stg_departments
├── intermediate/     # Business logic, joins, aggregations
│   ├── int_orders_enriched
│   ├── int_user_order_history
│   └── int_products_enriched
└── marts/            # Final facts and dimensions, BI-ready
    ├── fct_orders
    ├── fct_order_products
    ├── dim_users
    ├── dim_products
    └── dim_date
```

## 📈 Metrics Defined

| Metric | Definition | Type |
|---|---|---|
| DAU | Distinct users placing at least one order per day | Simple |
| WAU | Distinct users placing at least one order per week | Simple |
| Avg Order Size | Average number of products per order | Simple |
| Reorder Rate | % of order-product rows where is_reordered = true | Simple |
| Orders Per User | Average orders placed per user | Derived |
| D7 Retention | % of users who placed another order within 7 days | Ratio |

All metrics are defined in the dbt Semantic Layer using MetricFlow YAML — 
queryable from any connected BI tool via a single consistent definition.

---

## 🗂️ Dataset

**Source:** [Instacart Online Grocery Shopping Dataset](https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset)

| Table | Description | Rows |
|---|---|---|
| orders | One row per order | ~3.4M |
| order_products | One row per product per order | ~33M |
| products | Product master list | 49,688 |
| aisles | Aisle lookup | 134 |
| departments | Department lookup | 21 |

**Note:** No real calendar dates exist in the raw data. Order dates are 
synthesized by chaining `days_since_prior_order` from an anchor date of 
2024-01-01 per user.

---

## 🛠️ How to Run Locally

### Prerequisites
- Python 3.11+
- Conda or virtualenv
- Snowflake account (free trial at snowflake.com/try)
- Kaggle account (to download dataset)

### 1. Clone the repo
```bash
git clone https://github.com/nithinpradeep38/product-analytics-platform.git
cd product-analytics-platform
```

### 2. Create environment and install dependencies
```bash
conda create -n dbtenv python=3.11 -y
conda activate dbtenv
pip install -r requirements.txt
```

### 3. Set up credentials
```bash
cp .env.example .env
# Fill in your Snowflake credentials in .env
```

### 4. Set up Snowflake
Run the DDL in `docs/snowflake_setup.sql` to create the warehouse, 
database, schemas, and roles.

### 5. Load raw data
Download the Instacart dataset from Kaggle and place CSVs in `/data`, then:
```bash
python ingestion/load_instacart.py
```

### 6. Run dbt
```bash
cd dbt/instacart_analytics
dbt deps
dbt build
```

### 7. Run Streamlit locally
```bash
cd /path/to/product-analytics-platform
streamlit run streamlit/app.py
```

---

## 🧪 Testing

All dbt models have tests defined in `schema.yml` files:
- **Staging:** unique + not_null on all primary keys
- **Marts:** relationship tests, dbt-expectations range checks
- **CI:** GitHub Actions runs `dbt build` on every PR to main

---

## 📁 Project Structure


---

```text
product-analytics-platform/
├── .github/workflows/    # GitHub Actions CI/CD
├── airflow/dags/         # Airflow pipeline DAG
├── dbt/instacart_analytics/  # dbt project
│   ├── macros/           # Custom Jinja macros
│   ├── models/           # Staging, intermediate, marts
│   └── packages.yml      # dbt-utils, dbt-expectations
├── docs/                 # Architecture diagram + dbt docs
├── ingestion/            # Python ingestion script
└── streamlit/            # Streamlit dashboard
```