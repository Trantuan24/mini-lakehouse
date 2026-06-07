"""Bronze ingest: load 9 sources into Iceberg tables under database `bronze`.

Source is configurable:
  INGEST_SOURCE=postgres (default, extension #3)  -> read from olist_source schema
  INGEST_SOURCE=csv                               -> read CSVs directly (fallback)

No business transforms. Two metadata columns are added to every table:
  _ingested_at (timestamp), _source_file (string).
Write mode is overwrite (idempotent re-runs)."""
import os
import sys

sys.path.insert(0, "/opt/pipeline")

from pyspark.sql.functions import current_timestamp, lit, days, col
from common.spark_session import get_spark, ensure_databases
from common.config import (CSV_TO_TABLE, BRONZE_PARTITIONED, DATASET_DIR,
                           pg_jdbc_url, PG_SOURCE_SCHEMA, PG_PROPERTIES)

INGEST_SOURCE = os.environ.get("INGEST_SOURCE", "postgres")


def read_source(spark, table, csv_name):
    if INGEST_SOURCE == "csv":
        path = f"{DATASET_DIR}/{csv_name}"
        print(f"  reading CSV {path}")
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    # postgres
    dbtable = f"{PG_SOURCE_SCHEMA}.{table}"
    print(f"  reading JDBC {dbtable}")
    return (spark.read.format("jdbc")
            .option("url", pg_jdbc_url())
            .option("dbtable", dbtable)
            .options(**PG_PROPERTIES)
            .load())


def main():
    spark = get_spark("bronze_ingest")
    ensure_databases(spark)

    for csv_name, table in CSV_TO_TABLE.items():
        print(f"\n[bronze] {table}  (from {INGEST_SOURCE})")
        df = read_source(spark, table, csv_name)
        df = (df.withColumn("_ingested_at", current_timestamp())
                .withColumn("_source_file", lit(csv_name)))

        writer = df.writeTo(f"bronze.{table}").using("iceberg") \
                   .tableProperty("format-version", "2")
        if table in BRONZE_PARTITIONED:
            writer = writer.partitionedBy(days(col("_ingested_at")))
        writer.createOrReplace()
        print(f"  wrote bronze.{table}: {df.count():,} rows")

    print("\nBronze ingest complete.")
    spark.stop()


if __name__ == "__main__":
    main()
