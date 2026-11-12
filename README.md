# AWS E-Commerce Data Pipeline

Built an end-to-end data pipeline using AWS services to ingest, process, transform, and analyze e-commerce data.

## Tech Stack
- Python
- SQL
- PySpark
- AWS S3
- AWS Glue
- Amazon Athena
- Git & GitHub
- CSV and Parquet

## Architecture

```text
Olist CSV Datasets
       |
       v
   Amazon S3
  Bronze Layer
       |
       v
   AWS Glue
  Silver Layer
   (Parquet)
       |
       v
   AWS Glue
   Gold Layer
 (Fact & Dimension Tables)
       |
       v
 Amazon Athena
  SQL Analytics

## Pipeline Architecture

### 1. Bronze Layer
- Uploaded 9 raw CSV datasets to Amazon S3.
- Organized raw data into separate entity-based folders.

### 2. Silver Layer
- Processed raw datasets using AWS Glue (PySpark).
- Performed data cleaning, type conversions, null handling, and deduplication.
- Converted CSV files into Parquet format.
- Stored transformed data in the S3 Silver layer.

### 3. Gold Layer
- Created 5 analytical tables:
  - `fact_order_items`
  - `fact_order_payments`
  - `dim_customers`
  - `dim_products`
  - `dim_sellers`
- Implemented fact and dimension table modelling.

### 4. Workflow Orchestration
- Created an AWS Glue Workflow with 6 ETL jobs.
- Configured job dependencies and success-based triggers.
- Executed the pipeline through workflow orchestration.

### 5. Data Validation & Analytics
- Validated source and target record counts.
- Checked duplicates, nulls, and referential integrity.
- Queried Gold tables using Amazon Athena.
- Performed sales, revenue, payment, product, seller, customer, delivery, and cancellation analysis.

## Dataset
Brazilian E-Commerce Public Dataset by Olist — [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
