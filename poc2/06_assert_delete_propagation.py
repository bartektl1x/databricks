"""
Validates the complete POC 2 deletion path:

    DQX-valid DELETE
      -> Delta Change Data Feed
      -> AUTO CDC SCD Type 2 closure
      -> Gold current-state removal

Expected state for C002:
    DQX-valid current rows: 0
    DQX-valid CDF deletes:  1
    Silver history rows:    1
    Silver active rows:     0
    Silver closed rows:     1
    Gold current rows:      0

This proves logical SCD2 deletion. It does not prove physical erasure of the
historical Silver record.
"""

from typing import Any

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"

DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.poc2_satellite_customers_scd2"
GOLD_VIEW = f"{CATALOG}.{SCHEMA}.poc2_gold_customers_current"

TEST_CUSTOMER_ID = "C002"

EXPECTED_ACTIVE_CUSTOMER_IDS = {
    "C001",
    "C004",
}


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


def collect_values(
    dataframe: DataFrame,
    column_name: str,
) -> set[Any]:
    return {
        row[column_name]
        for row in dataframe.select(column_name).collect()
    }


def find_latest_delete_commit(
    table_name: str,
) -> Row:
    """
    Return the most recent Delta DELETE commit.

    This isolated POC performs exactly one explicit DELETE after reset, so the
    latest DELETE commit is the deterministic commit under test.
    """

    delete_commit = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .filter(col("operation") == "DELETE")
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

    assert delete_commit is not None, (
        f"Expected at least one DELETE commit in Delta history for "
        f"{table_name}."
    )

    return delete_commit


# =============================================================================
# RESOLVE THE EXACT DELETE COMMIT
# =============================================================================

delete_commit = find_latest_delete_commit(
    DQX_VALID_TABLE
)

delete_commit_version = int(
    delete_commit["version"]
)

deleted_row_count = int(
    (delete_commit["operationMetrics"] or {}).get(
        "numDeletedRows",
        "0",
    )
)

assert_equal(
    deleted_row_count,
    1,
    "Rows deleted by the latest DQX-valid DELETE commit",
)


# =============================================================================
# LOAD CURRENT TABLE STATE
# =============================================================================

valid_rows = (
    spark.table(DQX_VALID_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
)

silver_history = (
    spark.table(SILVER_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
)

silver_active = silver_history.filter(
    col("__END_AT").isNull()
)

silver_closed = silver_history.filter(
    col("__END_AT").isNotNull()
)

gold_rows = (
    spark.table(GOLD_VIEW)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
)


# =============================================================================
# READ CDF ONLY FOR THE EXACT DELETE COMMIT
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


# =============================================================================
# ASSERT DELETE PROPAGATION
# =============================================================================

assert_equal(
    valid_rows.count(),
    0,
    f"DQX-valid row count for {TEST_CUSTOMER_ID}",
)

assert_equal(
    delete_events.count(),
    1,
    f"CDF delete-event count for {TEST_CUSTOMER_ID}",
)

assert_equal(
    silver_history.count(),
    1,
    f"Silver history row count for {TEST_CUSTOMER_ID}",
)

assert_equal(
    silver_active.count(),
    0,
    f"Active Silver row count for {TEST_CUSTOMER_ID}",
)

assert_equal(
    silver_closed.count(),
    1,
    f"Closed Silver row count for {TEST_CUSTOMER_ID}",
)

assert_equal(
    gold_rows.count(),
    0,
    f"Gold row count for {TEST_CUSTOMER_ID}",
)


# =============================================================================
# ASSERT CDF METADATA
# =============================================================================

delete_event = delete_events.first()

assert delete_event is not None, (
    f"Expected one CDF delete event for {TEST_CUSTOMER_ID}."
)

assert_equal(
    delete_event["_commit_version"],
    delete_commit_version,
    "CDF delete commit version",
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
# ASSERT RETAINED SILVER HISTORY
# =============================================================================

closed_row = (
    silver_closed
    .select(
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
    )
    .first()
)

assert closed_row is not None, (
    f"Expected one closed Silver row for {TEST_CUSTOMER_ID}."
)

assert_equal(
    closed_row["customer_id"],
    "C002",
    "Retained customer ID",
)

assert_equal(
    closed_row["name"],
    "Bob Smith",
    "Retained customer name",
)

assert_equal(
    closed_row["email"],
    "bob@example.com",
    "Retained customer email",
)

assert_equal(
    closed_row["city"],
    "Los Angeles",
    "Retained customer city",
)

assert closed_row["updated_at"] is not None, (
    "Expected retained updated_at."
)

assert closed_row["_ingested_at"] is not None, (
    "Expected retained _ingested_at."
)

assert_equal(
    closed_row["customer_id_passed"],
    True,
    "Retained customer_id quality flag",
)

assert_equal(
    closed_row["email_passed"],
    True,
    "Retained email quality flag",
)

assert_equal(
    closed_row["updated_at_passed"],
    True,
    "Retained updated_at quality flag",
)

assert closed_row["__START_AT"] is not None, (
    "Expected __START_AT to be populated."
)

assert closed_row["__END_AT"] is not None, (
    "Expected __END_AT to be populated after DELETE propagation."
)

assert closed_row["__END_AT"] > closed_row["__START_AT"], (
    "Expected __END_AT to be greater than __START_AT."
)


# =============================================================================
# ASSERT UNAFFECTED ACTIVE CUSTOMERS
# =============================================================================

actual_active_silver_customer_ids = collect_values(
    spark.table(SILVER_TABLE).filter(
        col("__END_AT").isNull()
    ),
    "customer_id",
)

assert_equal(
    actual_active_silver_customer_ids,
    EXPECTED_ACTIVE_CUSTOMER_IDS,
    "Active Silver customer IDs",
)

actual_gold_customer_ids = collect_values(
    spark.table(GOLD_VIEW),
    "customer_id",
)

assert_equal(
    actual_gold_customer_ids,
    EXPECTED_ACTIVE_CUSTOMER_IDS,
    "Gold customer IDs",
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2 DELETE-PROPAGATION ASSERTIONS PASSED")
print("=" * 80)
print(f"Customer:                {TEST_CUSTOMER_ID}")
print(f"DELETE commit version:   {delete_commit_version}")
print("DQX-valid rows:          0")
print("DQX-valid CDF deletes:   1")
print("Silver history rows:     1")
print("Silver active rows:      0")
print("Silver closed rows:      1")
print("Gold rows for customer:  0")
print()
print("Conclusion:")
print(
    "The DQX-valid DELETE propagated through Delta CDF and AUTO CDC. "
    "The active SCD2 version was closed, Gold stopped exposing C002, "
    "and the historical Silver attributes remained stored."
)
print("=" * 80)

silver_history.orderBy(
    "__START_AT"
).show(
    truncate=False
)
