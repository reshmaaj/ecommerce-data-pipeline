import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER = "s3://reshma-de-learning-2026-rj/ecommerce-platform/silver/"
GOLD = "s3://reshma-de-learning-2026-rj/ecommerce-platform/gold/"

customers = spark.read.parquet(SILVER + "customers/")

if customers.filter(col("customer_id").isNull()).limit(1).count() > 0:
    raise ValueError("Null customer_id found")

duplicates = (
    customers
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .limit(1)
    .count()
)

if duplicates > 0:
    raise ValueError("Duplicate customer_id values found")

source_count = customers.count()

print("SOURCE CUSTOMER COUNT:", source_count)

dim_customers = customers.select(
    col("customer_id"),
    col("customer_unique_id"),
    col("customer_zip_code_prefix"),
    col("customer_city"),
    col("customer_state")
)

gold_count = dim_customers.count()

print("GOLD CUSTOMER COUNT:", gold_count)

if gold_count != source_count:
    raise ValueError("Customer row count mismatch")

output_path = GOLD + "dim_customers/"

dim_customers.write.mode("overwrite").parquet(output_path)

print("GOLD DIM CUSTOMERS CREATED SUCCESSFULLY")
print("OUTPUT:", output_path)

job.commit()