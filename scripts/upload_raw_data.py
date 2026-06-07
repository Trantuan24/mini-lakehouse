"""Upload Olist CSV files from ./dataset to the MinIO `raw` bucket.

Usage (from host, needs boto3):  python scripts/upload_raw_data.py
The pipeline itself reads CSVs from the mounted /opt/dataset volume, so this
step exists mainly to satisfy the 'CSV in MinIO raw bucket' acceptance criterion.
"""
import os
import glob
import boto3
from botocore.client import Config

ENDPOINT = os.environ.get("S3_ENDPOINT_HOST", "http://localhost:9000")
ACCESS = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
DATASET_DIR = os.environ.get("DATASET_DIR", "dataset")
BUCKET = "raw"


def main():
    s3 = boto3.client("s3", endpoint_url=ENDPOINT,
                      aws_access_key_id=ACCESS, aws_secret_access_key=SECRET,
                      config=Config(signature_version="s3v4"), region_name="us-east-1")
    try:
        s3.create_bucket(Bucket=BUCKET)
    except Exception:
        pass

    files = glob.glob(os.path.join(DATASET_DIR, "*.csv"))
    if not files:
        print(f"No CSVs in {DATASET_DIR}")
        return
    for path in files:
        key = f"olist/{os.path.basename(path)}"
        print(f"  uploading {path} -> s3://{BUCKET}/{key}")
        s3.upload_file(path, BUCKET, key)
    print(f"Uploaded {len(files)} files to bucket '{BUCKET}'.")


if __name__ == "__main__":
    main()
