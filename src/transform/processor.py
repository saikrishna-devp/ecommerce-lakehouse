import boto3
import pandas as pd
import awswrangler as wr
import yaml
from datetime import datetime


def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


def process_orders(df):
    print("  Processing orders...")
    df = df.drop_duplicates(subset=["order_id"])
    df = df.dropna(subset=["order_id", "customer_id"])
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
    df["order_delivered_customer_date"] = pd.to_datetime(df["order_delivered_customer_date"], errors="coerce")
    df["order_estimated_delivery_date"] = pd.to_datetime(df["order_estimated_delivery_date"], errors="coerce")
    df["year"]  = df["order_purchase_timestamp"].dt.year
    df["month"] = df["order_purchase_timestamp"].dt.month
    df["day"]   = df["order_purchase_timestamp"].dt.day
    df = df[df["order_status"].isin(["delivered","shipped","canceled","invoiced","processing"])]
    print("  Orders processed: " + str(len(df)) + " rows")
    return df


def process_customers(df):
    print("  Processing customers...")
    df = df.drop_duplicates(subset=["customer_id"])
    df = df.dropna(subset=["customer_id","customer_state"])
    df["customer_state"] = df["customer_state"].str.upper().str.strip()
    print("  Customers processed: " + str(len(df)) + " rows")
    return df


def process_products(df):
    print("  Processing products...")
    df = df.drop_duplicates(subset=["product_id"])
    df = df.dropna(subset=["product_id"])
    df["product_weight_g"]  = pd.to_numeric(df["product_weight_g"],  errors="coerce").fillna(0)
    df["product_length_cm"] = pd.to_numeric(df["product_length_cm"], errors="coerce").fillna(0)
    df["product_height_cm"] = pd.to_numeric(df["product_height_cm"], errors="coerce").fillna(0)
    df["product_width_cm"]  = pd.to_numeric(df["product_width_cm"],  errors="coerce").fillna(0)
    print("  Products processed: " + str(len(df)) + " rows")
    return df


def process_order_items(df):
    print("  Processing order items...")
    df = df.dropna(subset=["order_id","product_id"])
    df["price"]             = pd.to_numeric(df["price"],             errors="coerce").fillna(0)
    df["freight_value"]     = pd.to_numeric(df["freight_value"],     errors="coerce").fillna(0)
    df["total_value"]       = df["price"] + df["freight_value"]
    df = df[df["price"] > 0]
    print("  Order items processed: " + str(len(df)) + " rows")
    return df


def process_payments(df):
    print("  Processing payments...")
    df = df.dropna(subset=["order_id"])
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").fillna(0)
    df = df[df["payment_value"] > 0]
    print("  Payments processed: " + str(len(df)) + " rows")
    return df


def run_processor(raw_data):
    config  = load_config()
    bucket  = config["aws"]["bucket"]
    prefix  = config["aws"]["processed_prefix"]
    region  = config["aws"]["region"]
    today   = datetime.utcnow().strftime("year=%Y/month=%m/day=%d")

    print("=" * 60)
    print("STEP 3 - TRANSFORM: Processing data to S3 Processed Layer")
    print("=" * 60)

    processors = {
        "orders":      process_orders,
        "customers":   process_customers,
        "products":    process_products,
        "order_items": process_order_items,
        "payments":    process_payments,
    }

    processed = {}
    for name, func in processors.items():
        if name not in raw_data:
            print("  Skipping " + name + " - not in raw data")
            continue

        df = func(raw_data[name].copy())
        s3_path = "s3://" + bucket + "/" + prefix + name + "/" + today + "/"

        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
        )
        print("  Saved to " + s3_path)
        processed[name] = df

    print("\nProcessed layer complete!")
    return processed
