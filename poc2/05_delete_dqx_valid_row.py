"""
Creates a deterministic DELETE in the DQX-valid table and verifies that the
exact DELETE commit produced one Change Data Feed event.

After this script succeeds:
    1. Run a NORMAL INCREMENTAL update of the CDC/Gold pipeline.
    2. Run 06_assert_delete_propagation.py.

This script intentionally fails when C002 is already absent so an accidental
rerun cannot hide an invalid test state.
"""

from typing import Any

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"

DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
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


def get_latest_history_row(
    table_name: str,
) -> Row:
    history_row = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .select(
            "version",
            "timestamp",
            "operation",
            "operationParameters",
            "operationMetrics",
        )
        .orderBy(col("version").desc())
        .first()
    )

    assert history_row is not None, (
        f"Expected Delta history for {table_name}."
    )

    return history_row


# =============================================================================
# ASSERT PRECONDITION
# =============================================================================

before_count = (
    spark.table(DQX_VALID_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .count()
)

assert_equal(
    before_count,
    1,
    f"DQX-valid row count for {TEST_CUSTOMER_ID} before deletion",
)


# =============================================================================
# CAPTURE PRE-DELETE VERSION
# =============================================================================

before_history = get_latest_history_row(
    DQX_VALID_TABLE
)

before_version = int(before_history["version"])


# =============================================================================
# DELETE DQX-VALID RECORD
# =============================================================================

spark.sql(
    f"""
    DELETE FROM {DQX_VALID_TABLE}
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
    """
)


# =============================================================================
# CAPTURE AND VALIDATE DELETE COMMIT
# =============================================================================

after_history = get_latest_history_row(
    DQX_VALID_TABLE
)

delete_commit_version = int(after_history["version"])

assert delete_commit_version > before_version, (
    "Expected DELETE to create a new Delta table version. "
    f"Version before DELETE: {before_version}; "
    f"version after DELETE: {delete_commit_version}."
)

assert_equal(
    after_history["operation"],
    "DELETE",
    "Latest Delta operation after deleting the test customer",
)

deleted_row_count = int(
    (after_history["operationMetrics"] or {}).get(
        "numDeletedRows",
        "0",
    )
)

assert_equal(
    deleted_row_count,
    1,
    "Number of rows reported deleted by Delta history",
)


# =============================================================================
# VERIFY CURRENT TABLE STATE
# =============================================================================

after_count = (
    spark.table(DQX_VALID_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .count()
)

assert_equal(
    after_count,
    0,
    f"DQX-valid row count for {TEST_CUSTOMER_ID} after deletion",
)


# =============================================================================
# VERIFY CDF EVENT FOR THE EXACT DELETE COMMIT
# =============================================================================

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
    FROM table_changes(
        '{DQX_VALID_TABLE}',
        {delete_commit_version},
        {delete_commit_version}
    )
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
      AND _change_type = 'delete'
    """
)

delete_event_count = delete_events.count()

assert_equal(
    delete_event_count,
    1,
    (
        f"CDF delete-event count for {TEST_CUSTOMER_ID} "
        f"at Delta version {delete_commit_version}"
    ),
)


# =============================================================================
# VERIFY CDF METADATA
# =============================================================================

delete_event = delete_events.first()

assert delete_event is not None, (
    f"Expected one CDF delete event for {TEST_CUSTOMER_ID}."
)

assert_equal(
    delete_event["_commit_version"],
    delete_commit_version,
    "CDF commit version",
)

assert_equal(
    delete_event["_change_type"],
    "delete",
    "CDF change type",
)

assert delete_event["_commit_timestamp"] is not None, (
    "Expected the CDF delete event to contain _commit_timestamp."
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2 DQX-VALID DELETE CREATED")
print("=" * 80)
print(f"Customer:              {TEST_CUSTOMER_ID}")
print(f"Version before DELETE: {before_version}")
print(f"DELETE commit version: {delete_commit_version}")
print("DQX-valid rows:        0")
print("CDF delete events:     1")
print()
print("Next steps:")
print("1. Run a NORMAL INCREMENTAL update of the CDC/Gold pipeline.")
print("2. Run 06_assert_delete_propagation.py.")
print("=" * 80)

delete_events.orderBy(
    "_commit_version",
    "_change_type",
).show(
    truncate=False
)
