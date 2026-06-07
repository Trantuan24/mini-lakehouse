"""Create the lakehouse MinIO buckets (raw, bronze, silver, gold, platinum,
warehouse, meta). Normally handled automatically by the `minio-init` service;
this script is for manual/standalone use."""
import os
import boto3
from botocore.client import Config

ENDPOINT = os.environ.get("S3_ENDPOINT_HOST", "http://localhost:9000")
ACCESS = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
BUCKETS = ["raw", "bronze", "silver", "gold", "platinum", "warehouse", "meta"]


def main():
    s3 = boto3.client("s3", endpoint_url=ENDPOINT,
                      aws_access_key_id=ACCESS, aws_secret_access_key=SECRET,
                      config=Config(signature_version="s3v4"), region_name="us-east-1")
    for b in BUCKETS:
        try:
            s3.create_bucket(Bucket=b)
            print(f"  created bucket {b}")
        except Exception as e:
            print(f"  bucket {b}: {e}")


if __name__ == "__main__":
    main()
