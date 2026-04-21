import awswrangler as wr
import boto3
import yaml


def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


def create_athena_database(config):
    db      = config["athena"]["database"]
    output  = config["athena"]["output_location"]
    region  = config["aws"]["region"]

    print("Creating Athena database: " + db)
    wr.athena.start_query_execution(
        sql="CREATE DATABASE IF NOT EXISTS " + db,
        s3_output=output,
        boto3_session=boto3.Session(region_name=region)
    )
    print("Database created!")


def create_curated_tables(config, processed):
    bucket  = config["aws"]["bucket"]
    prefix  = config["aws"]["curated_prefix"]
    db      = config["athena"]["database"]
    output  = config["athena"]["output_location"]
    region  = config["aws"]["region"]
    session = boto3.Session(region_name=region)

    print("=" * 60)
    print("STEP 4 - LOAD: Creating curated tables in Athena")
    print("=" * 60)

    if "orders" in processed and "order_items" in processed and "payments" in processed:
        print("Building revenue summary...")
        orders   = processed["orders"].copy()
        items    = processed["order_items"].copy()
        payments = processed["payments"].copy()

        revenue = payments.groupby("order_id").agg(
            total_payment=("payment_value","sum")
        ).reset_index()

        items_summary = items.groupby("order_id").agg(
            total_items=("order_item_id","count"),
            total_price=("price","sum"),
            total_freight=("freight_value","sum")
        ).reset_index()

        orders_enriched = orders.merge(revenue, on="order_id", how="left")
        orders_enriched = orders_enriched.merge(items_summary, on="order_id", how="left")

        s3_path = "s3://" + bucket + "/" + prefix + "revenue_summary/"
        wr.s3.to_parquet(
            df=orders_enriched,
            path=s3_path,
            dataset=True,
        )
        print("  Revenue summary saved to " + s3_path)

    if "order_items" in processed and "products" in processed:
        print("Building product performance...")
        items    = processed["order_items"].copy()
        products = processed["products"].copy()

        product_perf = items.groupby("product_id").agg(
            total_orders=("order_id","count"),
            total_revenue=("price","sum"),
            avg_price=("price","mean")
        ).reset_index()

        product_perf = product_perf.merge(products[["product_id","product_category_name"]], on="product_id", how="left")
        product_perf["avg_price"] = product_perf["avg_price"].round(2)
        product_perf["total_revenue"] = product_perf["total_revenue"].round(2)

        s3_path = "s3://" + bucket + "/" + prefix + "product_performance/"
        wr.s3.to_parquet(
            df=product_perf,
            path=s3_path,
            dataset=True,
        )
        print("  Product performance saved to " + s3_path)

    if "customers" in processed and "orders" in processed:
        print("Building customer summary...")
        customers = processed["customers"].copy()
        orders    = processed["orders"].copy()

        customer_orders = orders.groupby("customer_id").agg(
            total_orders=("order_id","count"),
            first_order=("order_purchase_timestamp","min"),
            last_order=("order_purchase_timestamp","max")
        ).reset_index()

        customer_summary = customers.merge(customer_orders, on="customer_id", how="left")
        customer_summary["total_orders"] = customer_summary["total_orders"].fillna(0)

        s3_path = "s3://" + bucket + "/" + prefix + "customer_summary/"
        wr.s3.to_parquet(
            df=customer_summary,
            path=s3_path,
            dataset=True,
        )
        print("  Customer summary saved to " + s3_path)

    print("\nCurated layer complete!")


def run_athena_queries(config):
    db      = config["athena"]["database"]
    output  = config["athena"]["output_location"]
    bucket  = config["aws"]["bucket"]
    region  = config["aws"]["region"]
    session = boto3.Session(region_name=region)

    print("=" * 60)
    print("STEP 5 - ATHENA QUERIES")
    print("=" * 60)

    queries = {
        "Total orders by status": """
            SELECT order_status,
                   COUNT(*) as total_orders
            FROM ecommerce_db.orders
            GROUP BY order_status
            ORDER BY total_orders DESC
        """,
    }

    for title, sql in queries.items():
        print("\n  " + title)
        try:
            df = wr.athena.read_sql_query(
                sql=sql,
                database=db,
                s3_output=output,
                boto3_session=session
            )
            print(df.to_string(index=False))
        except Exception as e:
            print("  Query error: " + str(e))
