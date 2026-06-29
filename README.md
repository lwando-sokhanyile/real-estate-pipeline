markdown# Real Estate Market Pipeline

A PySpark-based ETL pipeline that processes a USA housing dataset, applies data cleaning and schema casting, partitions the data by neighborhood, and stores it in AWS S3 as Parquet files.

## Tech Stack

- Python 3.11
- PySpark 3.5.3
- AWS S3
- boto3
- python-dotenv

## Pipeline Architecture
Raw CSV → PySpark Ingestion → Data Cleaning → Partitioned Parquet → AWS S3

## Project Structure
real-estate-pipeline/

├── src/

│   ├── ingest.py        # Reads raw CSV using PySpark

│   ├── clean.py         # Handles nulls, type casting, deduplication

│   └── load_to_s3.py    # Partitions by Neighborhood and uploads to S3

├── data/

│   ├── raw/             # Raw housing dataset (not committed)

│   └── processed/       # Cleaned Parquet files (not committed)

├── .env.example         # Environment variable template

├── .gitignore

└── README.md

## What It Does

1. **Ingestion** — Reads 1,459 rows of USA housing data from CSV using PySpark
2. **Cleaning** — Casts 11 numeric columns from string to double, drops duplicates, drops rows with null critical fields
3. **Partitioning and Load** — Writes data as Parquet files partitioned by Neighborhood (25 partitions) and uploads to AWS S3 using boto3

## Setup

1. Clone the repo:
```bash
   git clone https://github.com/lwando-sokhanyile/real-estate-pipeline.git
   cd real-estate-pipeline
```

2. Install dependencies:
```bash
   pip install pyspark==3.5.3 boto3 python-dotenv
```

3. Copy `.env.example` to `.env` and fill in your AWS credentials:
AWS_ACCESS_KEY_ID=your-key

AWS_SECRET_ACCESS_KEY=your-secret

AWS_DEFAULT_REGION=eu-west-1

S3_BUCKET=s3a://your-bucket-name

4. Add the raw dataset to `data/raw/housing_test.csv`

5. Run the pipeline in order:
```bash
   python src/ingest.py
   python src/clean.py
   python src/load_to_s3.py
```

## S3 Output Structure
s3://real-estate-pipeline-lwando/

└── processed/

└── housing_partitioned/

├── Neighborhood=NAmes/

├── Neighborhood=Gilbert/

├── Neighborhood=CollgCr/

└── ... (25 neighborhoods total)

## Key Design Decisions

- **Partitioned by Neighborhood** to enable efficient querying by location without full table scans
- **Parquet format** for columnar storage and compression — industry standard for data pipelines
- **Credentials managed via .env** — never hardcoded or committed to Git
- **boto3 for S3 upload** — more reliable than Spark's hadoop-aws connector in local mode on Windows
