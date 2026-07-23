"""
01_setup_source.py

Creates and seeds the Delta source table used by POC 1.

Run this script outside the Lakeflow pipeline.

Important:
    This script drops and recreates the source table. After rerunning it,
    perform a FULL REFRESH of the POC pipeline rather than a normal
    incremental update.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.source_customers"


# =============================================================================
# CREATE SCHEMA
# =============================================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")


# =============================================================================
# RECREATE SOURCE TABLE
# =============================================================================

spark.sql(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")

spark.sql(
    f"""
    CREATE TABLE {SOURCE_TABLE} (
        customer_id STRING NOT NULL,
        name        STRING,
        email       STRING,
        city        STRING,
        updated_at  TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
    """
)


# =============================================================================
# INSERT TEST DATA
# =============================================================================

spark.sql(
    f"""
    INSERT INTO {SOURCE_TABLE}
    VALUES
        (
            'C001',
            'Alice Johnson',
            'alice@example.com',
            'New York',
            TIMESTAMP '2024-01-01 10:00:00'
        ),
        (
            'C002',
            'Bob Smith',
            'bob@example.com',
            'Los Angeles',
            TIMESTAMP '2024-01-01 11:00:00'
        ),
        (
            'C003',
            'Charlie Brown',
            'charlie@example.com',
            'Chicago',
            TIMESTAMP '2024-01-01 12:00:00'
        ),
        (
            'C004',
            'Diana Prince',
            'diana@example.com',
            'Miami',
            TIMESTAMP '2024-01-01 13:00:00'
        )
    """
)


# =============================================================================
# VALIDATE SETUP
# =============================================================================

source_df = spark.table(SOURCE_TABLE)

actual_count = source_df.count()

assert actual_count == 4, (
    f"Expected 4 source rows after setup, but found {actual_count}."
)

actual_customer_ids = {
    row["customer_id"]
    for row in source_df.select("customer_id").collect()
}

expected_customer_ids = {"C001", "C002", "C003", "C004"}

assert actual_customer_ids == expected_customer_ids, (
    "Unexpected customer IDs after setup. "
    f"Expected {expected_customer_ids}, found {actual_customer_ids}."
)

cdf_properties = (
    spark.sql(f"SHOW TBLPROPERTIES {SOURCE_TABLE}")
    .filter(col("key") == "delta.enableChangeDataFeed")
    .collect()
)

assert len(cdf_properties) == 1, (
    "The delta.enableChangeDataFeed property was not found."
)

assert cdf_properties[0]["value"].lower() == "true", (
    "Change Data Feed is not enabled on the source table."
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("SOURCE TABLE CREATED SUCCESSFULLY")
print("=" * 80)
print(f"Table: {SOURCE_TABLE}")
print(f"Rows:  {actual_count}")
print()
print("Next step:")
print("Run a FULL REFRESH of the POC Lakeflow pipeline.")
print("=" * 80)

source_df.orderBy("customer_id").show(truncate=False)
