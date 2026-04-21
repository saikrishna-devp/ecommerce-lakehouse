import pandas as pd


def validate_dataset(name, df):
    print("  Validating " + name + "...")
    checks = []

    def check(desc, passed, detail=""):
        status = "PASS" if passed else "FAIL"
        print("    [" + status + "] " + desc + " " + detail)
        checks.append(passed)

    check("Row count valid",
          len(df) > 0,
          "(" + str(len(df)) + " rows)")

    if name == "orders":
        check("No null order_ids",
              df["order_id"].notna().all(),
              "(" + str(df["order_id"].isna().sum()) + " nulls)")
        check("No duplicate order_ids",
              df["order_id"].nunique() == len(df),
              "(" + str(df["order_id"].nunique()) + " unique)")
        check("Valid order status",
              df["order_status"].isin(["delivered","shipped","canceled","invoiced","processing","approved","created","unavailable"]).all(),
              "(found: " + str(df["order_status"].unique().tolist()) + ")")

    if name == "customers":
        check("No null customer_ids",
              df["customer_id"].notna().all(),
              "(" + str(df["customer_id"].isna().sum()) + " nulls)")
        check("No duplicate customer_ids",
              df["customer_id"].nunique() == len(df),
              "(" + str(df["customer_id"].nunique()) + " unique)")

    if name == "order_items":
        check("No null order_ids",
              df["order_id"].notna().all(),
              "(" + str(df["order_id"].isna().sum()) + " nulls)")
        check("Price is positive",
              (pd.to_numeric(df["price"], errors="coerce") > 0).all(),
              "(min=" + str(pd.to_numeric(df["price"], errors="coerce").min()) + ")")

    if name == "payments":
        check("No null order_ids",
              df["order_id"].notna().all(),
              "(" + str(df["order_id"].isna().sum()) + " nulls)")
        check("Payment value positive",
              (pd.to_numeric(df["payment_value"], errors="coerce") > 0).all(),
              "(min=" + str(pd.to_numeric(df["payment_value"], errors="coerce").min()) + ")")

    if name == "products":
        check("No null product_ids",
              df["product_id"].notna().all(),
              "(" + str(df["product_id"].isna().sum()) + " nulls)")

    passed = sum(checks)
    total  = len(checks)
    print("    Result: " + str(passed) + "/" + str(total) + " checks passed")
    return passed == total


def validate_all(datasets):
    print("=" * 60)
    print("STEP 2 - VALIDATE: Running data quality checks")
    print("=" * 60)
    results = {}
    for name, df in datasets.items():
        results[name] = validate_dataset(name, df)
    passed = sum(results.values())
    total  = len(results)
    print("\nValidation complete: " + str(passed) + "/" + str(total) + " datasets passed")
    return results
