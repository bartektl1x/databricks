"""
Create one deterministic DQX-valid UPDATE and verify its exact CDF events.

After this script:
    1. Run a NORMAL INCREMENTAL update of Pipeline 2 only.
    2. Run 06_assert_update_propagation.py.

Do not run Pipeline 1 during the update/delete test sequence.
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

ORIGINAL_EMAIL = "bob@example.com"
ORIGINAL_CITY = "Los Angeles"
UPDATED_EMAIL = "bob.updated@example.com"
UPDATED_CITY = "Seattle"
UPDATED_AT_SQL = "2026-07-21 11:00:00"


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
    f"DQX-valid row count for {TEST_CUSTOMER_ID} before update",
)
assert_equal(
    before_rows[0]["email"],
    ORIGINAL_EMAIL,
    "C002 email before update",
)
assert_equal(
    before_rows[0]["city"],
    ORIGINAL_CITY,
    "C002 city before update",
)

before_version = get_latest_version(DQX_VALID_TABLE)


# =============================================================================
# UPDATE DQX-VALID RECORD
# =============================================================================

spark.sql(
    f"""
    UPDATE {DQX_VALID_TABLE}
    SET
        email = '{UPDATED_EMAIL}',
        city = '{UPDATED_CITY}',
        updated_at = TIMESTAMP '{UPDATED_AT_SQL}'
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
    """
)

update_commit = find_new_operation_commit(
    DQX_VALID_TABLE,
    before_version,
    "UPDATE",
)
update_commit_version = int(update_commit["version"])


# =============================================================================
# VERIFY CURRENT TABLE STATE
# =============================================================================

after_row = (
    spark.table(DQX_VALID_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .select("email", "city", "updated_at")
    .first()
)

assert after_row is not None, (
    f"Expected {TEST_CUSTOMER_ID} to remain in DQX valid after UPDATE."
)
assert_equal(after_row["email"], UPDATED_EMAIL, "C002 email after update")
assert_equal(after_row["city"], UPDATED_CITY, "C002 city after update")
assert after_row["updated_at"] is not None, (
    "Expected updated_at to remain populated after update."
)


# =============================================================================
# VERIFY EXACT CDF UPDATE EVENTS
# =============================================================================

update_events = spark.sql(
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
        {update_commit_version},
        {update_commit_version}
    )
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
      AND _change_type IN ('update_preimage', 'update_postimage')
    """
)

assert_equal(
    update_events.count(),
    2,
    f"CDF update-event count at version {update_commit_version}",
)
assert_equal(
    {
        row["_change_type"]
        for row in update_events.select("_change_type").collect()
    },
    {
        "update_preimage",
        "update_postimage",
    },
    "CDF update change types",
)

postimage = (
    update_events
    .filter(col("_change_type") == "update_postimage")
    .first()
)
assert postimage is not None, "Expected one update_postimage event."
assert_equal(postimage["email"], UPDATED_EMAIL, "CDF postimage email")
assert_equal(postimage["city"], UPDATED_CITY, "CDF postimage city")
assert_equal(
    int(postimage["_commit_version"]),
    update_commit_version,
    "CDF update commit version",
)
assert postimage["_commit_timestamp"] is not None


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2.1 DQX-VALID UPDATE CREATED")
print("=" * 80)
print(f"Customer:              {TEST_CUSTOMER_ID}")
print(f"Version before UPDATE: {before_version}")
print(f"UPDATE commit version: {update_commit_version}")
print(f"Updated email:         {UPDATED_EMAIL}")
print(f"Updated city:          {UPDATED_CITY}")
print("CDF update events:     2")
print()
print("Next steps:")
print("1. Run a NORMAL INCREMENTAL update of Pipeline 2 only.")
print("2. Run 06_assert_update_propagation.py.")
print("=" * 80)

update_events.orderBy("_change_type").show(truncate=False)

