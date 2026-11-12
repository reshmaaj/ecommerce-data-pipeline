import boto3
from pathlib import Path


BUCKET = "reshma-de-learning-2026-rj"
S3_PREFIX = "ecommerce-platform/bronze"

DATA_DIR = Path("data/raw")

s3 = boto3.client("s3", region_name="ap-southeast-2")

for file in DATA_DIR.glob("*.csv"):

    dataset_name = file.stem.replace("olist_", "").replace("_dataset", "")

    s3_key = f"{S3_PREFIX}/{dataset_name}/{file.name}"

    print(f"Uploading: {file.name}")

    s3.upload_file(
        str(file),
        BUCKET,
        s3_key
    )

    print(f"Uploaded to: s3://{BUCKET}/{s3_key}")

print("\nAll files uploaded successfully!")