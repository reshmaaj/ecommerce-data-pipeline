import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER = "s3://reshma-de-learning-2026-rj/ecommerce-platform/silver/"
GOLD = "s3://reshma-de-learning-2026-rj/ecommerce-platform/gold/"

order_items = spark.read.parquet(SILVER + "order_items/")
orders = spark.read.parquet(SILVER + "orders/")
customers = spark.read.parquet(SILVER + "customers/")
products = spark.read.parquet(SILVER + "products/")
sellers = spark.read.parquet(SILVER + "sellers/")

def check_unique(df, key, dataset_name):

    if df.filter(col(key).isNull()).limit(1).count() > 0:
        raise ValueError(f"Null values found in {dataset_name}.{key}")

    duplicates = (
        df.groupBy(key)
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicates > 0:
        raise ValueError(f"Duplicate keys found in {dataset_name}.{key}")

    print(f"PASS: {dataset_name}.{key} is unique")


check_unique(order_items, "order_id", "order_items") if False else None

check_unique(orders, "order_id", "orders")
check_unique(customers, "customer_id", "customers")
check_unique(products, "product_id", "products")
check_unique(sellers, "seller_id", "sellers")

duplicate_order_items = (
    order_items
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(col("count") > 1)
    .limit(1)
    .count()
)

if duplicate_order_items > 0:
    raise ValueError("Duplicate order_id + order_item_id combinations found")

source_count = order_items.count()

print("SOURCE ORDER ITEMS COUNT:", source_count)

joined = (
    order_items.alias("oi")

    .join(
        orders.alias("o"),
        col("oi.order_id") == col("o.order_id"),
        "left"
    )

    .join(
        customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )

    .join(
        products.alias("p"),
        col("oi.product_id") == col("p.product_id"),
        "left"
    )

    .join(
        sellers.alias("s"),
        col("oi.seller_id") == col("s.seller_id"),
        "left"
    )
)

missing_references = joined.filter(
    col("o.order_id").isNull()
    | col("c.customer_id").isNull()
    | col("p.product_id").isNull()
    | col("s.seller_id").isNull()
).limit(1).count()

if missing_references > 0:
    raise ValueError("Unmatched foreign keys found during joins")

fact_order_items = joined.select(

    col("oi.order_id"),
    col("oi.order_item_id"),
    col("o.customer_id"),
    col("o.order_status"),

    to_date(col("o.order_purchase_timestamp")).alias("order_purchase_date"),
    col("o.order_purchase_timestamp"),
    col("o.order_approved_at"),
    col("o.order_delivered_carrier_date"),
    col("o.order_delivered_customer_date"),
    col("o.order_estimated_delivery_date"),

    col("oi.product_id"),
    col("oi.seller_id"),

    col("oi.price"),
    col("oi.freight_value"),
    (col("oi.price") + col("oi.freight_value")).alias("total_item_cost"),

    col("c.customer_unique_id"),
    col("c.customer_city"),
    col("c.customer_state"),

    col("p.product_category_name"),
    col("p.product_weight_g"),
    col("p.product_length_cm"),
    col("p.product_height_cm"),
    col("p.product_width_cm"),

    col("s.seller_city"),
    col("s.seller_state"),

    col("oi.shipping_limit_date")
)

gold_count = fact_order_items.count()

print("GOLD FACT ORDER ITEMS COUNT:", gold_count)

if gold_count != source_count:
    raise ValueError(
        f"Row count mismatch: source={source_count}, gold={gold_count}"
    )

output_path = GOLD + "fact_order_items/"

fact_order_items.write.mode("overwrite").parquet(output_path)

print("GOLD FACT ORDER ITEMS CREATED SUCCESSFULLY")
print("OUTPUT:", output_path)

job.commit()