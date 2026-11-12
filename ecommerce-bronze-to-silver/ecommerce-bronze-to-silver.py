
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, expr


# Initialize Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# S3 paths
BRONZE = "s3://reshma-de-learning-2026-rj/ecommerce-platform/bronze/"
SILVER = "s3://reshma-de-learning-2026-rj/ecommerce-platform/silver/"


# Data type conversions
casts = {
    "customers": {
        "customer_zip_code_prefix": "string"
    },
    "geolocation": {
        "geolocation_zip_code_prefix": "string",
        "geolocation_lat": "double",
        "geolocation_lng": "double"
    },
    "order_items": {
        "order_item_id": "int",
        "price": "double",
        "freight_value": "double"
    },
    "order_payments": {
        "payment_sequential": "int",
        "payment_installments": "int",
        "payment_value": "double"
    },
    "order_reviews": {
        "review_score": "int"
    },
    "products": {
        "product_name_lenght": "int",
        "product_description_lenght": "int",
        "product_photos_qty": "int",
        "product_weight_g": "double",
        "product_length_cm": "double",
        "product_height_cm": "double",
        "product_width_cm": "double"
    },
    "sellers": {
        "seller_zip_code_prefix": "string"
    }
}


# All nine datasets
datasets = [
    "customers",
    "geolocation",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "product_category_name_translation"
]


# Bronze to Silver processing
for name in datasets:

    print(f"Processing: {name}")

    # Read CSV from Bronze
    df = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("multiLine", "true")
        .csv(f"{BRONZE}{name}/")
        )
        
    for name in datasets:
        print(f"Processing: {name}")

    # Read CSV from Bronze
        df = (
            spark.read
            .option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .csv(f"{BRONZE}{name}/")
        )

    # TEMPORARY CHECK
        if name == "order_reviews":
            print("ORDER REVIEWS COUNT:", df.count())

    # Apply data type conversions
        for column, data_type in casts.get(name, {}).items():
            df = df.withColumn(
                column,
                expr(f"try_cast(`{column}` AS {data_type})")
            )

    # Apply data type conversions
    for column, data_type in casts.get(name, {}).items():

        df = df.withColumn(
            column,
            expr(f"try_cast(`{column}` AS {data_type})")
        )

    # Remove duplicate geolocation records
    if name == "geolocation":
        df = df.dropDuplicates()

    # Remove duplicate orders
    if name == "orders":
        df = df.dropDuplicates(["order_id"])

    if name == "order_reviews":
        print("ORDER REVIEWS BEFORE WRITE:", df.count())
    
    # Write Silver data as Parquet
    output_path = f"{SILVER}{name}/"

    df.write.mode("overwrite").parquet(output_path)

    print(f"Completed: {name}")


print("All 9 datasets processed successfully!")

job.commit()
