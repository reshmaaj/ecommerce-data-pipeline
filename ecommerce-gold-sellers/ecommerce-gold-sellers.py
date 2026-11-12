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

sellers = spark.read.parquet(SILVER + "sellers/")

if sellers.filter(col("seller_id").isNull()).limit(1).count() > 0:
    raise ValueError("Null seller_id found")

duplicates = (
    sellers
    .groupBy("seller_id")
    .count()
    .filter(col("count") > 1)
    .limit(1)
    .count()
)

if duplicates > 0:
    raise ValueError("Duplicate seller_id values found")

source_count = sellers.count()

print("SOURCE SELLER COUNT:", source_count)

dim_sellers = sellers.select(
    col("seller_id"),
    col("seller_zip_code_prefix"),
    col("seller_city"),
    col("seller_state")
)

gold_count = dim_sellers.count()

print("GOLD SELLER COUNT:", gold_count)

if gold_count != source_count:
    raise ValueError("Seller row count mismatch")

output_path = GOLD + "dim_sellers/"

dim_sellers.write.mode("overwrite").parquet(output_path)

print("GOLD DIM SELLERS CREATED SUCCESSFULLY")
print("OUTPUT:", output_path)

job.commit()