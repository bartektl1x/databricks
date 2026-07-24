"""
Create the deterministic C002 DQX-valid DELETE and verify its exact CDF event.

Run only after 06_assert_update_propagation.py succeeds.

After this script:
    1. Run a NORMAL INCREMENTAL update of Pipeline 2 only.
    2. Run 08_assert_delete_propagation.py.
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

DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc21_dqx_customers_valid"
TEST_CUSTOMER_ID = "C002"

EXPECTED_EMAIL = "bob.updated@example.com"
EXPECTED_CITY = "Seattle"


# =============================================================================
# HELPERS
# =============================================================================

def assert_equal(
    actual: Any,
    expected: Any,
    description: str,
) -> None:
    assert actual == expected, (
        f"{description}: expected {expected!r}, found {actual!r}."
    )


def get_latest_version(table_name: str) -> int:
    row = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .select("version")
        .orderBy(col("version").desc())
        .first()
    )
    assert row is not None, f"Expected Delta history for {table_name}."
    return int(row["version"])


def find_new_operation_commit(
    table_name: str,
    previous_version: int,
    operation: str,
) -> Row:
    row = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .filter(col("version") > previous_version)
        .filter(col("operation") == operation)
        .select(
            "version",
            "timestamp",
            "operation",
            "operationParameters",
            "operationMetrics",
        )
        .orderBy(col("version").asc())
        .first()
    )
    assert row is not None, (
        f"Expected a new {operation} commit in {table_name} after "
        f"version {previous_version}."
    )
    return row


# =============================================================================
# PRECONDITION
# =============================================================================

before_rows = (
    spark.table(DQX_VALID_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .select("email", "city")
    .collect()
)

assert_equal(
    len(before_rows),
    1,
    f"DQX-valid row count for {TEST_CUSTOMER_ID} before deletion",
)
assert_equal(
    before_rows[0]["email"],
    EXPECTED_EMAIL,
    "C002 email before deletion",
)
assert_equal(
    before_rows[0]["city"],
    EXPECTED_CITY,
    "C002 city before deletion",
)

before_version = get_latest_version(DQX_VALID_TABLE)


# =============================================================================
# DELETE DQX-VALID RECORD
# =============================================================================

spark.sql(
    f"""
    DELETE FROM {DQX_VALID_TABLE}
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
    """
)

delete_commit = find_new_operation_commit(
    DQX_VALID_TABLE,
    before_version,
    "DELETE",
)
delete_commit_version = int(delete_commit["version"])


# =============================================================================
# VERIFY CURRENT STATE
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
# VERIFY EXACT CDF DELETE EVENT
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

assert_equal(
    delete_events.count(),
    1,
    (
        f"CDF delete-event count for {TEST_CUSTOMER_ID} at "
        f"version {delete_commit_version}"
    ),
)

delete_event = delete_events.first()
assert delete_event is not None
assert_equal(delete_event["email"], EXPECTED_EMAIL, "CDF deleted email")
assert_equal(delete_event["city"], EXPECTED_CITY, "CDF deleted city")
assert_equal(delete_event["_change_type"], "delete", "CDF change type")
assert_equal(
    int(delete_event["_commit_version"]),
    delete_commit_version,
    "CDF delete commit version",
)
assert delete_event["_commit_timestamp"] is not None


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2.1 DQX-VALID DELETE CREATED")
print("=" * 80)
print(f"Customer:              {TEST_CUSTOMER_ID}")
print(f"Version before DELETE: {before_version}")
print(f"DELETE commit version: {delete_commit_version}")
print("DQX-valid rows:        0")
print("CDF delete events:     1")
print()
print("Next steps:")
print("1. Run a NORMAL INCREMENTAL update of Pipeline 2 only.")
print("2. Run 08_assert_delete_propagation.py.")
print("=" * 80)

delete_events.show(truncate=False)

