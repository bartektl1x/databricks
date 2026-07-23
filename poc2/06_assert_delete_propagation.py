"""
Validates POC 2 after the CDC/Gold pipeline processes the DQX-valid DELETE.

Expected C002 state:
    DQX-valid current rows: 0
    DQX-valid CDF deletes:  1
    Satellite history:      1
    Satellite active:       0
    Satellite closed:       1
    Gold current rows:      0

This proves logical SCD2 deletion, not physical historical erasure.
"""

from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "dev_mr_dhc_bronze"
SCHEMA = "slpat_landing_staging"

DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
SATELLITE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_satellite_customers_scd2"
GOLD_VIEW = f"{CATALOG}.{SCHEMA}.poc2_gold_customers_current"

TEST_CUSTOMER_ID = "C002"
EXPECTED_ACTIVE_IDS = {"C001", "C004"}


# =============================================================================
# HELPERS
# =============================================================================

def assert_equal(actual: Any, expected: Any, description: str) -> None:
    assert actual == expected, (
        f"{description}: expected {expected!r}, found {actual!r}."
    )


# =============================================================================
# CURRENT STATE
# =============================================================================

valid_rows = (
    spark.table(DQX_VALID_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
)

satellite_history = (
    spark.table(SATELLITE_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
)

satellite_active = satellite_history.filter(col("__END_AT").isNull())
satellite_closed = satellite_history.filter(col("__END_AT").isNotNull())

gold_rows = (
    spark.table(GOLD_VIEW)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
)

delete_events = spark.sql(
    f"""
    SELECT
        customer_id,
        name,
        email,
        city,
        updated_at,
        _ingested_at,
        _change_type,
        _commit_version,
        _commit_timestamp
    FROM table_changes('{DQX_VALID_TABLE}', 0)
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
      AND _change_type = 'delete'
    """
)


# =============================================================================
# ASSERT DELETE PROPAGATION
# =============================================================================

assert_equal(valid_rows.count(), 0, "DQX-valid row count after deletion")
assert_equal(delete_events.count(), 1, "DQX-valid CDF delete-event count")
assert_equal(satellite_history.count(), 1, "Satellite history row count")
assert_equal(satellite_active.count(), 0, "Active satellite row count")
assert_equal(satellite_closed.count(), 1, "Closed satellite row count")
assert_equal(gold_rows.count(), 0, "Gold current row count for C002")


# =============================================================================
# ASSERT RETAINED SCD2 HISTORY
# =============================================================================

closed_row = satellite_closed.select(
    "customer_id",
    "name",
    "email",
    "city",
    "updated_at",
    "_ingested_at",
    "customer_id_passed",
    "email_passed",
    "updated_at_passed",
    "__START_AT",
    "__END_AT",
).first()

assert closed_row is not None, "Expected one closed C002 satellite row."

assert_equal(closed_row["name"], "Bob Smith", "Retained customer name")
assert_equal(closed_row["email"], "bob@example.com", "Retained customer email")
assert_equal(closed_row["city"], "Los Angeles", "Retained customer city")
assert closed_row["_ingested_at"] is not None
assert closed_row["customer_id_passed"] is True
assert closed_row["email_passed"] is True
assert closed_row["updated_at_passed"] is True
assert closed_row["__START_AT"] is not None
assert closed_row["__END_AT"] is not None
assert closed_row["__END_AT"] > closed_row["__START_AT"]


# =============================================================================
# ASSERT UNAFFECTED ACTIVE STATE
# =============================================================================

actual_active_satellite_ids = {
    row["customer_id"]
    for row in (
        spark.table(SATELLITE_TABLE)
        .filter(col("__END_AT").isNull())
        .select("customer_id")
        .collect()
    )
}

actual_gold_ids = {
    row["customer_id"]
    for row in spark.table(GOLD_VIEW).select("customer_id").collect()
}

assert_equal(
    actual_active_satellite_ids,
    EXPECTED_ACTIVE_IDS,
    "Active satellite customer IDs",
)

assert_equal(actual_gold_ids, EXPECTED_ACTIVE_IDS, "Gold customer IDs")


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2 DELETE-PROPAGATION ASSERTIONS PASSED")
print("=" * 80)
print(f"Customer:                {TEST_CUSTOMER_ID}")
print("DQX-valid rows:          0")
print("DQX-valid CDF deletes:   1")
print("Satellite history rows:  1")
print("Satellite active rows:   0")
print("Satellite closed rows:   1")
print("Gold rows for customer:  0")
print()
print("Conclusion:")
print(
    "The DQX-valid DELETE propagated through Delta CDF and AUTO CDC. "
    "The SCD2 version was closed, Gold stopped exposing the customer, and "
    "the historical satellite attributes remained stored."
)
print("=" * 80)

satellite_history.orderBy("__START_AT").show(truncate=False)
