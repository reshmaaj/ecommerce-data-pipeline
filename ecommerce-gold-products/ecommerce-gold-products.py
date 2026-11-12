import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, coalesce

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER = "s3://reshma-de-learning-2026-rj/ecommerce-platform/silver/"
GOLD = "s3://reshma-de-learning-2026-rj/ecommerce-platform/gold/"

products = spark.read.parquet(SILVER + "products/")
categories = spark.read.parquet(
    SILVER + "product_category_name_translation/"
)

if products.filter(col("product_id").isNull()).limit(1).count() > 0:
    raise ValueError("Null product_id found")

duplicate_products = (
    products
    .groupBy("product_id")
    .count()
    .filter(col("count") > 1)
    .limit(1)
    .count()
)

if duplicate_products > 0:
    raise ValueError("Duplicate product_id values found")

duplicate_categories = (
    categories
    .groupBy("product_category_name")
    .count()
    .filter(col("count") > 1)
    .limit(1)
    .count()
)

if duplicate_categories > 0:
    raise ValueError("Duplicate product category mappings found")

source_count = products.count()

print("SOURCE PRODUCT COUNT:", source_count)

joined = products.alias("p").join(
    categories.alias("t"),
    col("p.product_category_name")
    == col("t.product_category_name"),
    "left"
)

dim_products = joined.select(
    col("p.product_id"),
    col("p.product_category_name"),
    coalesce(
        col("t.product_category_name_english"),
        col("p.product_category_name")
    ).alias("product_category_name_english"),
    col("p.product_name_lenght"),
    col("p.product_description_lenght"),
    col("p.product_photos_qty"),
    col("p.product_weight_g"),
    col("p.product_length_cm"),
    col("p.product_height_cm"),
    col("p.product_width_cm")
)

gold_count = dim_products.count()

print("GOLD PRODUCT COUNT:", gold_count)

if gold_count != source_count:
    raise ValueError(
        f"Row count mismatch: source={source_count}, gold={gold_count}"
    )

output_path = GOLD + "dim_products/"

dim_products.write.mode("overwrite").parquet(output_path)

print("GOLD DIM PRODUCTS CREATED SUCCESSFULLY")
print("OUTPUT:", output_path)

job.commit()