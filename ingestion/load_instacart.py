import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
print("USER:", os.getenv("SNOWFLAKE_USER"))
print("ACCOUNT:", os.getenv("SNOWFLAKE_ACCOUNT"))

# Connection 
def get_connection():
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        role      = os.getenv("SNOWFLAKE_ROLE")
    )

# Date synthesis
def synthesize_order_dates(orders_df):
    """
    Reconstruct a real calendar date for each order using
    days_since_prior_order. Anchor date = 2024-01-01.
    """
    anchor = datetime(2024, 1, 1)
    orders_df = orders_df.sort_values(["user_id", "order_number"])
    
    order_dates = []
    
    for user_id, group in orders_df.groupby("user_id"):
        group = group.sort_values("order_number").reset_index(drop=True)
        dates = []
        for i, row in group.iterrows():
            if row["order_number"] == 1 or pd.isna(row["days_since_prior_order"]):
                dates.append(anchor)
            else:
                prev_date = dates[-1]
                delta = int(row["days_since_prior_order"])
                dates.append(prev_date + timedelta(days=delta))
        order_dates.extend(dates)
    
    orders_df = orders_df.reset_index(drop=True)
    orders_df["order_date"] = [d.date() for d in order_dates]
    return orders_df

# Upload helper 
def upload_table(conn, df, table_name):
    """Upload a dataframe to Snowflake RAW schema."""
    # Snowflake expects uppercase column names
    df.columns = [c.upper() for c in df.columns]
    
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        database="ANALYTICS_DEV",
        schema="RAW",
        overwrite=True
    )
    print(f"  ✓ {table_name}: {nrows:,} rows loaded")

# Main 
def main():
    # Point this to wherever you saved the Kaggle CSVs
    DATA_DIR = "data/"
    
    print("Connecting to Snowflake...")
    conn = get_connection()
    print("Connected.\n")

    # Small lookup tables first — validates connection before large loads
    print("Loading aisles...")
    aisles = pd.read_csv(f"{DATA_DIR}aisles.csv")
    upload_table(conn, aisles, "AISLES")

    print("Loading departments...")
    departments = pd.read_csv(f"{DATA_DIR}departments.csv")
    upload_table(conn, departments, "DEPARTMENTS")

    print("Loading products...")
    products = pd.read_csv(f"{DATA_DIR}products.csv")
    upload_table(conn, products, "PRODUCTS")

    # Orders — drop eval_set, synthesize order_date
    print("Loading orders (with date synthesis)...")
    orders = pd.read_csv(f"{DATA_DIR}orders.csv")
    orders = orders.drop(columns=["eval_set"])
    orders = synthesize_order_dates(orders)
    upload_table(conn, orders, "ORDERS")

    # Order products — union prior + train
    print("Loading order_products (union of prior + train)...")
    prior = pd.read_csv(f"{DATA_DIR}order_products__prior.csv")
    train = pd.read_csv(f"{DATA_DIR}order_products__train.csv")
    order_products = pd.concat([prior, train]).drop_duplicates(
        subset=["order_id", "product_id"]
    ).reset_index(drop=True)
    upload_table(conn, order_products, "ORDER_PRODUCTS")

    conn.close()
    print("\nAll tables loaded successfully.")

if __name__ == "__main__":
    main()