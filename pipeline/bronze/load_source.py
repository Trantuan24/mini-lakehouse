"""[Extension #3] Load raw Olist CSV files into Postgres `olist_source` schema
to simulate an OLTP source system. Bronze can then ingest from this DB via JDBC.

Run inside a container that has pandas + sqlalchemy + psycopg2 (the airflow image).
This job does NOT need Spark."""
import os
import sys
import glob

import pandas as pd
from sqlalchemy import create_engine

DATASET_DIR = os.environ.get("DATASET_DIR", "/opt/dataset")
PG_USER = os.environ.get("POSTGRES_USER", "airflow")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")
SOURCE_DB = os.environ.get("SOURCE_DB", "olist_source")
SCHEMA = "olist_source"

CSV_TO_TABLE = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}


def main():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@postgres:5432/{SOURCE_DB}"
    engine = create_engine(url)

    found = glob.glob(os.path.join(DATASET_DIR, "*.csv"))
    if not found:
        print(f"ERROR: no CSV files found in {DATASET_DIR}. "
              f"Place the Olist dataset there first.", file=sys.stderr)
        sys.exit(1)

    loaded = 0
    for csv_name, table in CSV_TO_TABLE.items():
        path = os.path.join(DATASET_DIR, csv_name)
        if not os.path.exists(path):
            print(f"  (skip) missing {csv_name}")
            continue
        print(f"  loading {csv_name} -> {SCHEMA}.{table} ...")
        df = pd.read_csv(path)
        df.to_sql(table, engine, schema=SCHEMA, if_exists="replace",
                  index=False, chunksize=10000, method="multi")
        print(f"    {len(df):,} rows")
        loaded += 1

    print(f"Done: loaded {loaded} tables into {SOURCE_DB}.{SCHEMA}")


if __name__ == "__main__":
    main()
