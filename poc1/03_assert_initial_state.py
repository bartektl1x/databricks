"""
03_assert_initial_state.py

Validates the state after the first successful FULL REFRESH of the pipeline.

Expected state for C002:
    Source rows:          1
    Target history rows:  1
    Target active rows:   1
    Target closed rows:   0
"""

from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
# ASSERTION HELPERS
# =============================================================================

def assert_equal(
    actual: Any,
    expected: Any,
    description: str,
) -> None:
    assert actual == expected, (
        f"{description}: expected {expected!r}, found {actual!r}."
    )


# =============================================================================
# LOAD TEST RECORDS
# =============================================================================

source_rows = (
    spark.table(SOURCE_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
)

target_history = (
    spark.table(TARGET_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
)

target_active = target_history.filter(col("__END_AT").isNull())
target_closed = target_history.filter(col("__END_AT").isNotNull())


# =============================================================================
# ASSERT INITIAL STATE
# =============================================================================

assert_equal(
    source_rows.count(),
    1,
    "Source row count before deletion",
)

assert_equal(
    target_history.count(),
    1,
    "SCD2 history row count before deletion",
)

assert_equal(
    target_active.count(),
    1,
    "Active SCD2 row count before deletion",
)

assert_equal(
    target_closed.count(),
    0,
    "Closed SCD2 row count before deletion",
)

active_row = target_active.select(
    "customer_id",
    "name",
    "email",
    "city",
    "updated_at",
    "__START_AT",
    "__END_AT",
).first()

assert active_row is not None, (
    f"Expected an active target row for {TEST_CUSTOMER_ID}."
)

assert_equal(
    active_row["customer_id"],
    "C002",
    "Customer ID",
)

assert_equal(
    active_row["name"],
    "Bob Smith",
    "Customer name",
)

assert_equal(
    active_row["email"],
    "bob@example.com",
    "Customer email",
)

assert_equal(
    active_row["city"],
    "Los Angeles",
    "Customer city",
)

assert active_row["__START_AT"] is not None, (
    "Expected __START_AT to be populated."
)

assert active_row["__END_AT"] is None, (
    "Expected __END_AT to be null before deletion."
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("INITIAL-STATE ASSERTIONS PASSED")
print("=" * 80)
print(f"Customer:             {TEST_CUSTOMER_ID}")
print("Source rows:          1")
print("Target history rows:  1")
print("Target active rows:   1")
print("Target closed rows:   0")
print()
print("Next step:")
print("Run 04_delete_source_row.py.")
print("=" * 80)

target_history.select(
    "customer_id",
    "name",
    "email",
    "city",
    "updated_at",
    "__START_AT",
    "__END_AT",
).show(truncate=False)
