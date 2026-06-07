"""Data quality gate on Bronze tables."""
import sys
sys.path.insert(0, "/opt/pipeline")

from common.spark_session import get_spark
from common.config import CSV_TO_TABLE, PRIMARY_KEYS
from common import data_quality as dq


def main():
    spark = get_spark("bronze_validate")
    results = []
    for table in CSV_TO_TABLE.values():
        df = spark.table(f"bronze.{table}")
        results.append(dq.expect_row_count_gt(df, 0, table=f"bronze.{table}", layer="bronze"))
        for pk in PRIMARY_KEYS.get(table, []):
            if pk in df.columns:
                results.append(dq.expect_column_not_null(df, pk, table=f"bronze.{table}", layer="bronze"))
    dq.run_suite(spark, "bronze", results)
    spark.stop()


if __name__ == "__main__":
    main()
