import os
import boto3
import pandas as pd
import yaml
from datetime import datetime


def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


def upload_to_s3_as_parquet(df, s3_client, bucket, key):
    local_path = "data/temp.parquet"
    df.to_parquet(local_path, index=False)
    s3_client.upload_file(local_path, bucket, key)
    os.remove(local_path)
    print("  Uploaded " + str(len(df)) + " rows -> s3://" + bucket + "/" + key)


def run_uploader(data_dir="data/kaggle/"):
    config   = load_config()
    bucket   = config["aws"]["bucket"]
    prefix   = config["aws"]["raw_prefix"]
    s3       = boto3.client("s3", region_name=config["aws"]["region"])
    today    = datetime.utcnow().strftime("year=%Y/month=%m/day=%d")

    print("=" * 60)
    print("STEP 1 - EXTRACT: Uploading Kaggle data to S3 Raw Layer")
    print("=" * 60)

    datasets = {
        "orders":       "olist_orders_dataset.csv",
        "customers":    "olist_customers_dataset.csv",
        "products":     "olist_products_dataset.csv",
        "order_items":  "olist_order_items_dataset.csv",
        "payments":     "olist_order_payments_dataset.csv",
        "reviews":      "olist_order_reviews_dataset.csv",
        "sellers":      "olist_sellers_dataset.csv",
    }

    uploaded = {}
    for name, filename in datasets.items():
        filepath = os.path.join(data_dir, filename)
        if not os.path.exists(filepath):
            print("  WARNING: " + filepath + " not found - skipping")
            continue

        print("Loading " + filename + "...")
        df = pd.read_csv(filepath)
        print("  Loaded " + str(len(df)) + " rows")

        key = prefix + name + "/" + today + "/data.parquet"
        upload_to_s3_as_parquet(df, s3, bucket, key)
        uploaded[name] = df

    print("\nRaw layer upload complete!")
    print("Uploaded " + str(len(uploaded)) + " datasets to s3://" + bucket + "/" + prefix)
    return uploaded
