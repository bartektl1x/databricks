"""
Validate POC 2.1 after the C002 UPDATE and a normal Pipeline 2 update.

The critical negative proof is that an ordinary SCD2 closure creates no
deletion marker and is classified with closed_by_delete = false.
"""

import hashlib
from typing import Any

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"

DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc21_dqx_customers_valid"
HUB_TABLE = f"{CATALOG}.{SCHEMA}.poc21_hub_customers"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.poc21_satellite_customers_scd2"
MARKER_TABLE = f"{CATALOG}.{SCHEMA}.poc21_customer_deletion_markers"
CLASSIFIED_VIEW = (
    f"{CATALOG}.{SCHEMA}.poc21_satellite_customers_classified"
)
GOLD_VIEW = f"{CATALOG}.{SCHEMA}.poc21_gold_customers_current"

TEST_CUSTOMER_ID = "C002"
ORIGINAL_EMAIL = "bob@example.com"
ORIGINAL_CITY = "Los Angeles"
UPDATED_EMAIL = "bob.updated@example.com"
UPDATED_CITY = "Seattle"


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


def collect_values(
    dataframe: DataFrame,
    column_name: str,
) -> set[Any]:
    return {
        row[column_name]
        for row in dataframe.select(column_name).collect()
    }


def find_latest_operation_commit(
    table_name: str,
    operation: str,
) -> Row:
    row = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .filter(col("operation") == operation)
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
    assert row is not None, (
        f"Expected a {operation} commit in Delta history for {table_name}."
    )
    return row


def expected_customer_hash_key(customer_id: str) -> str:
    normalized_value = f"CUSTOMER||{customer_id.strip().upper()}"
    return hashlib.sha256(normalized_value.encode("utf-8")).hexdigest()


# =============================================================================
# RESOLVE EXACT UPDATE VERSION
# =============================================================================

update_commit = find_latest_operation_commit(
    DQX_VALID_TABLE,
    "UPDATE",
)
update_commit_version = int(update_commit["version"])
expected_c002_hk = expected_customer_hash_key(TEST_CUSTOMER_ID)

delete_commit_count = (
    spark.sql(f"DESCRIBE HISTORY {DQX_VALID_TABLE}")
    .filter(col("operation") == "DELETE")
    .count()
)
assert_equal(
    delete_commit_count,
    0,
    "DQX-valid DELETE commits before the delete phase",
)

update_postimage = (
    spark.sql(
        f"""
        SELECT
            customer_id,
            email,
            city,
            _change_type,
            _commit_version,
            _commit_timestamp
        FROM table_changes(
            '{DQX_VALID_TABLE}',
            {update_commit_version},
            {update_commit_version}
        )
        WHERE customer_id = '{TEST_CUSTOMER_ID}'
          AND _change_type = 'update_postimage'
        """
    )
)
assert_equal(
    update_postimage.count(),
    1,
    "Exact CDF update_postimage count",
)


# =============================================================================
# HUB INVARIANCE
# =============================================================================

hub_df = spark.table(HUB_TABLE)
assert_equal(hub_df.count(), 3, "Hub row count after source update")

c002_hub_rows = (
    hub_df
    .filter(col("customer_hk") == expected_c002_hk)
)
assert_equal(c002_hub_rows.count(), 1, "C002 Hub rows after source update")

c002_hub = (
    c002_hub_rows
    .select(
        "customer_hk",
        "customer_id",
        "record_source",
        "hub_loaded_at",
    )
    .first()
)
assert c002_hub is not None
assert_equal(c002_hub["customer_id"], TEST_CUSTOMER_ID, "Hub business key")
assert_equal(
    c002_hub["record_source"],
    "POC21_DQX_VALID",
    "Hub record source after source update",
)
assert c002_hub["hub_loaded_at"] is not None
assert c002_hub["hub_loaded_at"] < update_commit["timestamp"], (
    "The Hub load timestamp must predate the attribute UPDATE. "
    "The Hub source is insert-only and must not be reloaded by the update."
)


# =============================================================================
# SATELLITE UPDATE HISTORY
# =============================================================================

silver_history = (
    spark.table(SILVER_TABLE)
    .filter(col("customer_hk") == expected_c002_hk)
)
silver_active = silver_history.filter(col("__END_AT").isNull())
silver_closed = silver_history.filter(col("__END_AT").isNotNull())

assert_equal(silver_history.count(), 2, "C002 Silver history rows after update")
assert_equal(silver_active.count(), 1, "C002 active Silver rows after update")
assert_equal(silver_closed.count(), 1, "C002 closed Silver rows after update")

closed_row = (
    silver_closed
    .select(
        "email",
        "city",
        "__START_AT",
        "__END_AT",
    )
    .first()
)
assert closed_row is not None
assert_equal(closed_row["email"], ORIGINAL_EMAIL, "Closed version email")
assert_equal(closed_row["city"], ORIGINAL_CITY, "Closed version city")
assert_equal(
    int(closed_row["__END_AT"]),
    update_commit_version,
    "Update-closed Silver __END_AT",
)

active_row = (
    silver_active
    .select(
        "email",
        "city",
        "__START_AT",
        "__END_AT",
    )
    .first()
)
assert active_row is not None
assert_equal(active_row["email"], UPDATED_EMAIL, "Active version email")
assert_equal(active_row["city"], UPDATED_CITY, "Active version city")
assert_equal(
    int(active_row["__START_AT"]),
    update_commit_version,
    "Updated Silver __START_AT",
)
assert active_row["__END_AT"] is None


# =============================================================================
# NO DELETE MARKER AFTER ORDINARY UPDATE
# =============================================================================

c002_markers = (
    spark.table(MARKER_TABLE)
    .filter(col("customer_hk") == expected_c002_hk)
)
assert_equal(
    c002_markers.count(),
    0,
    "C002 deletion markers after ordinary update",
)

classified_rows = (
    spark.table(CLASSIFIED_VIEW)
    .filter(col("customer_hk") == expected_c002_hk)
)
assert_equal(
    classified_rows.count(),
    2,
    "C002 classified Silver rows after update",
)
assert_equal(
    classified_rows.filter(col("closed_by_delete")).count(),
    0,
    "C002 delete-classified Silver rows after update",
)
assert_equal(
    classified_rows
    .filter(
        col("delete_sequence").isNotNull()
        | col("deletion_reason").isNotNull()
        | col("deletion_request_id").isNotNull()
    )
    .count(),
    0,
    "C002 rows with deletion metadata after update",
)


# =============================================================================
# GOLD AND UNAFFECTED CUSTOMERS
# =============================================================================

c002_gold = (
    spark.table(GOLD_VIEW)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
)
assert_equal(c002_gold.count(), 1, "C002 Gold rows after update")

c002_gold_row = c002_gold.select("email", "city").first()
assert c002_gold_row is not None
assert_equal(c002_gold_row["email"], UPDATED_EMAIL, "Gold C002 email")
assert_equal(c002_gold_row["city"], UPDATED_CITY, "Gold C002 city")

expected_active_ids = {
    "C001",
    "C002",
    "C004",
}
assert_equal(
    collect_values(
        spark.table(SILVER_TABLE).filter(col("__END_AT").isNull()),
        "customer_id",
    ),
    expected_active_ids,
    "Active Silver customer IDs after update",
)
assert_equal(
    collect_values(spark.table(GOLD_VIEW), "customer_id"),
    expected_active_ids,
    "Gold customer IDs after update",
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2.1 UPDATE-PROPAGATION ASSERTIONS PASSED")
print("=" * 80)
print(f"Customer:              {TEST_CUSTOMER_ID}")
print(f"UPDATE commit version: {update_commit_version}")
print("Hub C002 rows:         1")
print("Silver history rows:   2")
print("Silver active rows:    1")
print("Deletion markers:      0")
print("Delete-classified:     0")
print("Gold C002 rows:        1")
print()
print("The first C002 version closed because of an update, not a delete.")
print("Next: run 07_delete_dqx_valid_row.py.")
print("=" * 80)

classified_rows.orderBy("__START_AT").show(truncate=False)
