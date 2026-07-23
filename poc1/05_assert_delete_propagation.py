"""
05_assert_delete_propagation.py

Validates delete propagation after the NORMAL INCREMENTAL pipeline update.

Expected state for C002:
    Source rows:          0
    CDF delete events:   1
    Target history rows: 1
    Target active rows:  0
    Target closed rows:  1

This proves logical SCD2 deletion. It does not prove physical erasure.
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
# LOAD CURRENT STATE
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

delete_events = spark.sql(
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
      AND _change_type = 'delete'
    """
)


# =============================================================================
# ASSERT DELETE PROPAGATION
# =============================================================================

assert_equal(
    source_rows.count(),
    0,
    "Source row count after deletion",
)

assert_equal(
    delete_events.count(),
    1,
    "Source CDF delete-event count",
)

assert_equal(
    target_history.count(),
    1,
    "Total SCD2 history row count after delete propagation",
)

assert_equal(
    target_active.count(),
    0,
    "Active SCD2 row count after delete propagation",
)

assert_equal(
    target_closed.count(),
    1,
    "Closed SCD2 row count after delete propagation",
)


# =============================================================================
# ASSERT RETAINED HISTORY
# =============================================================================

closed_row = target_closed.select(
    "customer_id",
    "name",
    "email",
    "city",
    "updated_at",
    "__START_AT",
    "__END_AT",
).first()

assert closed_row is not None, (
    f"Expected one closed historical record for {TEST_CUSTOMER_ID}."
)

assert_equal(
    closed_row["customer_id"],
    "C002",
    "Closed customer ID",
)

assert_equal(
    closed_row["name"],
    "Bob Smith",
    "Retained historical customer name",
)

assert_equal(
    closed_row["email"],
    "bob@example.com",
    "Retained historical customer email",
)

assert_equal(
    closed_row["city"],
    "Los Angeles",
    "Retained historical customer city",
)

assert closed_row["__START_AT"] is not None, (
    "Expected the historical row to have __START_AT populated."
)

assert closed_row["__END_AT"] is not None, (
    "Expected the historical row to have __END_AT populated."
)

assert closed_row["__END_AT"] > closed_row["__START_AT"], (
    "Expected __END_AT to be greater than __START_AT."
)


# =============================================================================
# ASSERT UNAFFECTED CUSTOMERS
# =============================================================================

expected_active_customer_ids = {"C001", "C003", "C004"}

actual_active_customer_ids = {
    row["customer_id"]
    for row in (
        spark.table(TARGET_TABLE)
        .filter(col("__END_AT").isNull())
        .select("customer_id")
        .collect()
    )
}

assert actual_active_customer_ids == expected_active_customer_ids, (
    "Unexpected active customer set after deletion. "
    f"Expected {expected_active_customer_ids}, "
    f"found {actual_active_customer_ids}."
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("DELETE-PROPAGATION ASSERTIONS PASSED")
print("=" * 80)
print(f"Customer:             {TEST_CUSTOMER_ID}")
print("Source rows:          0")
print("CDF delete events:    1")
print("Target history rows:  1")
print("Target active rows:   0")
print("Target closed rows:   1")
print()
print("Conclusion:")
print(
    "The source DELETE propagated through Delta CDF and AUTO CDC. "
    "The active SCD2 version was closed, while its historical business "
    "attributes remained stored."
)
print("=" * 80)

target_history.orderBy("__START_AT").show(truncate=False)
