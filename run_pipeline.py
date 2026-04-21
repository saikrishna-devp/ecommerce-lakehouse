import os
import time
import yaml
from datetime import datetime


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(ts + " | " + msg)


def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    start  = time.time()
    config = load_config()

    os.makedirs("data/kaggle", exist_ok=True)
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/curated", exist_ok=True)

    log("=" * 60)
    log("  ECOMMERCE DATA LAKEHOUSE PIPELINE")
    log("  S3 + Athena + AWS Glue Architecture")
    log("  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log("=" * 60)

    log("Checking Kaggle data files...")
    kaggle_dir = "data/kaggle/"
    required_files = [
        "olist_orders_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_products_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
    ]
    missing = [f for f in required_files if not os.path.exists(kaggle_dir + f)]
    if missing:
        log("ERROR: Missing files in data/kaggle/: " + str(missing))
        log("Please copy the Kaggle CSV files to data/kaggle/ folder")
        exit(1)
    log("All required files found!")

    log("STEP 1 - Uploading raw data to S3...")
    from src.extract.uploader import run_uploader
    raw_data = run_uploader(data_dir=kaggle_dir)

    log("STEP 2 - Validating data quality...")
    from src.quality.validator import validate_all
    validate_all(raw_data)

    log("STEP 3 - Processing data to S3 Processed Layer...")
    from src.transform.processor import run_processor
    processed_data = run_processor(raw_data)

    log("STEP 4 - Building curated layer and Athena tables...")
    from src.load.athena_loader import create_athena_database, create_curated_tables
    create_athena_database(config)
    create_curated_tables(config, processed_data)

    elapsed = round(time.time() - start, 1)
    log("=" * 60)
    log("  PIPELINE COMPLETE in " + str(elapsed) + " seconds!")
    log("  Raw data:       s3://saikrishna-ecommerce-lakehouse/raw/")
    log("  Processed data: s3://saikrishna-ecommerce-lakehouse/processed/")
    log("  Curated data:   s3://saikrishna-ecommerce-lakehouse/curated/")
    log("=" * 60)
