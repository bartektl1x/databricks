"""
setup_source.py - Run this first to create and seed the source table.
Run in a Databricks notebook or job.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Create source table with CDF enabled
spark.sql(f"""
    DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.source_customers
""")

spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.source_customers (
        customer_id STRING,
        name STRING,
        email STRING,
        city STRING,
        updated_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Insert seed data
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.source_customers VALUES
        ('C001', 'Alice Johnson', 'alice@example.com', 'New York', '2024-01-01 10:00:00'),
        ('C002', 'Bob Smith', 'bob@example.com', 'Los Angeles', '2024-01-01 11:00:00'),
        ('C003', 'Charlie Brown', 'charlie@example.com', 'Chicago', '2024-01-01 12:00:00'),
        ('C004', 'Diana Prince', 'diana@example.com', 'Miami', '2024-01-01 13:00:00')
""")

print("Source table created and seeded.")
spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.source_customers").show(truncate=False)
