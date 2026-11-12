# # # import pandas as pd

# # # df = pd.read_csv("data/raw/olist_orders_dataset.csv")

# # # print("Order status distribution:")
# # # print(df["order_status"].value_counts())

# # # print("\nMissing delivery dates by order status:")

# # # columns = [
# # #     "order_approved_at",
# # #     "order_delivered_carrier_date",
# # #     "order_delivered_customer_date"
# # # ]

# # # print(df.groupby("order_status")[columns].apply(
# # #     lambda x: x.isnull().sum()
# # # ))

# # # ---------------------------- 

# # # import pandas as pd

# # # file_path = "data/raw/olist_order_reviews_dataset.csv"

# # # df = pd.read_csv(file_path)

# # # print("Total rows:", len(df))
# # # print("Unique review IDs:", df["review_id"].nunique())
# # # print("Unique order IDs:", df["order_id"].nunique())

# # # --------------------------------

# # import pandas as pd

# # file_path = "data/raw/olist_order_reviews_dataset.csv"

# # # Read the original CSV
# # df = pd.read_csv(file_path)

# # print("LOCAL CSV")
# # print("Total rows:", len(df))
# # print("Unique review IDs:", df["review_id"].nunique())

# # # Check the CSV structure
# # print("\nCOLUMNS:")
# # print(df.columns.tolist())

# # # Check for multiline review comments
# # multiline = df["review_comment_message"].dropna().str.contains(
# #     r"[\r\n]", regex=True
# # )

# # print("\nMultiline comments:", multiline.sum())

# import pandas as pd

# df = pd.read_csv("data/raw/olist_order_reviews_dataset.csv")

# print("Duplicate review_id + order_id pairs:",
#       df.duplicated(["review_id", "order_id"]).sum())

# print("Duplicate complete rows:",
#       df.duplicated().sum())

import boto3
import hashlib

local_file = "data/raw/olist_order_reviews_dataset.csv"

bucket = "reshma-de-learning-2026-rj"
key = "ecommerce-platform/bronze/order_reviews/olist_order_reviews_dataset.csv"

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

s3 = boto3.client("s3", region_name="ap-southeast-2")

response = s3.get_object(Bucket=bucket, Key=key)

s3_hash = hashlib.sha256(response["Body"].read()).hexdigest()
local_hash = sha256_file(local_file)

print("Local SHA256:", local_hash)
print("S3 SHA256:   ", s3_hash)
print("Files match:", local_hash == s3_hash)