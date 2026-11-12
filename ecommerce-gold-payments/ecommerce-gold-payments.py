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

payments = spark.read.parquet(SILVER + "order_payments/")
orders = spark.read.parquet(SILVER + "orders/")
customers = spark.read.parquet(SILVER + "customers/")

def check_unique(df, key, dataset_name):

    if df.filter(col(key).isNull()).limit(1).count() > 0:
        raise ValueError(f"Null key found in {dataset_name}.{key}")

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


check_unique(orders, "order_id", "orders")
check_unique(customers, "customer_id", "customers")

duplicate_payments = (
    payments
    .groupBy("order_id", "payment_sequential")
    .count()
    .filter(col("count") > 1)
    .limit(1)
    .count()
)

if duplicate_payments > 0:
    raise ValueError("Duplicate payment transactions found")

source_count = payments.count()

print("SOURCE PAYMENT COUNT:", source_count)

joined = (
    payments.alias("p")

    .join(
        orders.alias("o"),
        col("p.order_id") == col("o.order_id"),
        "left"
    )

    .join(
        customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )
)

missing_references = joined.filter(
    col("o.order_id").isNull()
    | col("c.customer_id").isNull()
).limit(1).count()

if missing_references > 0:
    raise ValueError("Unmatched order or customer references found")

fact_order_payments = joined.select(

    col("p.order_id"),
    col("p.payment_sequential"),

    col("o.customer_id"),
    col("c.customer_unique_id"),
    col("o.order_status"),

    to_date(
        col("o.order_purchase_timestamp")
    ).alias("order_purchase_date"),

    col("o.order_purchase_timestamp"),

    col("p.payment_type"),
    col("p.payment_installments"),
    col("p.payment_value")
)

gold_count = fact_order_payments.count()

print("GOLD PAYMENT COUNT:", gold_count)

if gold_count != source_count:
    raise ValueError(
        f"Row count mismatch: source={source_count}, gold={gold_count}"
    )

output_path = GOLD + "fact_order_payments/"

fact_order_payments.write.mode("overwrite").parquet(output_path)

print("GOLD FACT ORDER PAYMENTS CREATED SUCCESSFULLY")
print("OUTPUT:", output_path)

job.commit()