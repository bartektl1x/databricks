"""
06_show_evidence.py

Displays source history, source CDF events, and final SCD2 state.

This script contains no destructive actions and can be rerun safely.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.source_customers"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.satellite_customers_scd2"

TEST_CUSTOMER_ID = "C002"


# =============================================================================
# SOURCE CURRENT STATE
# =============================================================================

print()
print("=" * 80)
print("CURRENT SOURCE TABLE")
print("=" * 80)

spark.table(SOURCE_TABLE).orderBy("customer_id").show(truncate=False)


# =============================================================================
# SOURCE TABLE HISTORY
# =============================================================================

print()
print("=" * 80)
print("SOURCE DELTA HISTORY")
print("=" * 80)

spark.sql(
    f"""
    DESCRIBE HISTORY {SOURCE_TABLE}
    """
).select(
    "version",
    "timestamp",
    "operation",
    "operationParameters",
).orderBy(
    col("version").asc()
).show(
    truncate=False
)


# =============================================================================
# SOURCE CDF EVENTS FOR TEST CUSTOMER
# =============================================================================

print()
print("=" * 80)
print(f"SOURCE CDF EVENTS FOR {TEST_CUSTOMER_ID}")
print("=" * 80)

spark.sql(
    f"""
    SELECT
        customer_id,
        name,
        email,
        city,
        updated_at,
        _change_type,
        _commit_version,
        _commit_timestamp
    FROM table_changes('{SOURCE_TABLE}', 0)
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
    ORDER BY _commit_version, _change_type
    """
).show(truncate=False)


# =============================================================================
# TARGET SCD2 STATE
# =============================================================================

print()
print("=" * 80)
print("FINAL SCD2 TARGET STATE")
print("=" * 80)

target_df = spark.table(TARGET_TABLE)

(
    target_df
    .withColumn(
        "record_status",
        when(col("__END_AT").isNull(), "ACTIVE").otherwise("CLOSED"),
    )
    .select(
        "customer_id",
        "name",
        "email",
        "city",
        "updated_at",
        "__START_AT",
        "__END_AT",
        "record_status",
    )
    .orderBy(
        "customer_id",
        "__START_AT",
    )
    .show(truncate=False)
)


# =============================================================================
# TARGET TABLE HISTORY
# =============================================================================

print()
print("=" * 80)
print("TARGET DELTA HISTORY")
print("=" * 80)

spark.sql(
    f"""
    DESCRIBE HISTORY {TARGET_TABLE}
    """
).select(
    "version",
    "timestamp",
    "operation",
    "operationParameters",
).orderBy(
    col("version").asc()
).show(
    truncate=False
)
