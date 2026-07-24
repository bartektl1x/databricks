"""
Validate the complete POC 2.1 update-versus-delete contract.

Expected C002 result:

    update:
        first Satellite version closes
        second Satellite version becomes active
        no deletion marker exists

    delete:
        second Satellite version closes
        no replacement version is created
        Hub identity remains
        one durable deletion marker exists
        only the delete-closed version is classified as deletion
        Gold excludes C002

This script is safe to rerun after a no-new-input Pipeline 2 update. The marker
count must remain one.
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

EXPECTED_ACTIVE_CUSTOMER_IDS = {
    "C001",
    "C004",
}


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
# EXACT SOURCE COMMITS
# =============================================================================

update_commit = find_latest_operation_commit(
    DQX_VALID_TABLE,
    "UPDATE",
)
delete_commit = find_latest_operation_commit(
    DQX_VALID_TABLE,
    "DELETE",
)

update_version = int(update_commit["version"])
delete_version = int(delete_commit["version"])
expected_c002_hk = expected_customer_hash_key(TEST_CUSTOMER_ID)

assert delete_version > update_version, (
    "Expected the C002 DELETE commit to occur after the C002 UPDATE commit. "
    f"UPDATE version={update_version}, DELETE version={delete_version}."
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
    FROM table_changes(
        '{DQX_VALID_TABLE}',
        {delete_version},
        {delete_version}
    )
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
      AND _change_type = 'delete'
    """
)
assert_equal(delete_events.count(), 1, "Exact C002 CDF delete-event count")

delete_event = delete_events.first()
assert delete_event is not None
assert_equal(delete_event["email"], UPDATED_EMAIL, "Deleted CDF email")
assert_equal(delete_event["city"], UPDATED_CITY, "Deleted CDF city")
assert_equal(
    int(delete_event["_commit_version"]),
    delete_version,
    "Deleted CDF commit version",
)
assert delete_event["_commit_timestamp"] is not None


# =============================================================================
# DQX CURRENT STATE
# =============================================================================

assert_equal(
    spark.table(DQX_VALID_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .count(),
    0,
    "C002 DQX-valid rows after deletion",
)


# =============================================================================
# HUB MUST REMAIN
# =============================================================================

hub_df = spark.table(HUB_TABLE)
assert_equal(hub_df.count(), 3, "Hub row count after source deletion")

c002_hub = (
    hub_df
    .filter(col("customer_hk") == expected_c002_hk)
    .select(
        "customer_hk",
        "customer_id",
        "record_source",
        "hub_loaded_at",
    )
    .first()
)
assert c002_hub is not None, "Expected C002 Hub identity after source delete."
assert_equal(c002_hub["customer_id"], TEST_CUSTOMER_ID, "Hub business key")
assert_equal(
    c002_hub["record_source"],
    "POC21_DQX_VALID",
    "Hub record source after deletion",
)
assert c002_hub["hub_loaded_at"] is not None
assert c002_hub["hub_loaded_at"] < delete_commit["timestamp"], (
    "Hub load timestamp must predate the source DELETE. "
    "The delete must not replace or remove the Hub row."
)


# =============================================================================
# SATELLITE HISTORY
# =============================================================================

satellite_history = (
    spark.table(SILVER_TABLE)
    .filter(col("customer_hk") == expected_c002_hk)
)
satellite_active = satellite_history.filter(col("__END_AT").isNull())
satellite_closed = satellite_history.filter(col("__END_AT").isNotNull())

assert_equal(
    satellite_history.count(),
    2,
    "C002 total Satellite versions after deletion",
)
assert_equal(
    satellite_active.count(),
    0,
    "C002 active Satellite versions after deletion",
)
assert_equal(
    satellite_closed.count(),
    2,
    "C002 closed Satellite versions after deletion",
)

original_version = (
    satellite_history
    .filter(col("email") == ORIGINAL_EMAIL)
    .select(
        "customer_hk",
        "customer_id",
        "email",
        "city",
        "__START_AT",
        "__END_AT",
    )
    .first()
)
assert original_version is not None
assert_equal(original_version["city"], ORIGINAL_CITY, "Original version city")
assert_equal(
    int(original_version["__END_AT"]),
    update_version,
    "Original version update closure",
)

updated_version = (
    satellite_history
    .filter(col("email") == UPDATED_EMAIL)
    .select(
        "customer_hk",
        "customer_id",
        "email",
        "city",
        "__START_AT",
        "__END_AT",
    )
    .first()
)
assert updated_version is not None
assert_equal(updated_version["city"], UPDATED_CITY, "Updated version city")
assert_equal(
    int(updated_version["__START_AT"]),
    update_version,
    "Updated version start sequence",
)
assert_equal(
    int(updated_version["__END_AT"]),
    delete_version,
    "Updated version delete closure",
)


# =============================================================================
# DURABLE DELETE MARKER
# =============================================================================

c002_markers = (
    spark.table(MARKER_TABLE)
    .filter(col("customer_hk") == expected_c002_hk)
)

assert_equal(
    c002_markers.count(),
    1,
    "C002 durable deletion-marker count",
)
assert_equal(
    c002_markers
    .select("customer_hk", "delete_sequence")
    .distinct()
    .count(),
    1,
    "Distinct logical C002 deletion-marker count",
)

marker = c002_markers.select(
    "customer_hk",
    "customer_id",
    "delete_sequence",
    "deleted_at",
    "deletion_reason",
    "deletion_request_id",
).first()

assert marker is not None
assert_equal(marker["customer_hk"], expected_c002_hk, "Marker hash key")
assert_equal(marker["customer_id"], TEST_CUSTOMER_ID, "Marker business key")
assert_equal(
    int(marker["delete_sequence"]),
    delete_version,
    "Marker delete sequence",
)
assert_equal(
    marker["deleted_at"],
    delete_event["_commit_timestamp"],
    "Marker deleted_at",
)
assert_equal(
    marker["deletion_reason"],
    "SOURCE_DELETE",
    "Marker deletion reason",
)
assert marker["deletion_request_id"] is None, (
    "POC source deletes must have a null deletion_request_id."
)


# =============================================================================
# UPDATE-VERSUS-DELETE CLASSIFICATION
# =============================================================================

classified_history = (
    spark.table(CLASSIFIED_VIEW)
    .filter(col("customer_hk") == expected_c002_hk)
)
assert_equal(
    classified_history.count(),
    2,
    "C002 classified Satellite version count",
)
assert_equal(
    classified_history.filter(col("closed_by_delete")).count(),
    1,
    "C002 delete-classified version count",
)

classified_original = (
    classified_history
    .filter(col("email") == ORIGINAL_EMAIL)
    .select(
        "__END_AT",
        "closed_by_delete",
        "delete_sequence",
        "deleted_at",
        "deletion_reason",
        "deletion_request_id",
    )
    .first()
)
assert classified_original is not None
assert_equal(
    int(classified_original["__END_AT"]),
    update_version,
    "Update-closed classified version end",
)
assert_equal(
    classified_original["closed_by_delete"],
    False,
    "Update-closed version classification",
)
assert classified_original["delete_sequence"] is None
assert classified_original["deleted_at"] is None
assert classified_original["deletion_reason"] is None
assert classified_original["deletion_request_id"] is None

classified_updated = (
    classified_history
    .filter(col("email") == UPDATED_EMAIL)
    .select(
        "__END_AT",
        "closed_by_delete",
        "delete_sequence",
        "deleted_at",
        "deletion_reason",
        "deletion_request_id",
    )
    .first()
)
assert classified_updated is not None
assert_equal(
    int(classified_updated["__END_AT"]),
    delete_version,
    "Delete-closed classified version end",
)
assert_equal(
    classified_updated["closed_by_delete"],
    True,
    "Delete-closed version classification",
)
assert_equal(
    int(classified_updated["delete_sequence"]),
    delete_version,
    "Classified delete sequence",
)
assert_equal(
    classified_updated["deletion_reason"],
    "SOURCE_DELETE",
    "Classified deletion reason",
)
assert classified_updated["deletion_request_id"] is None


# =============================================================================
# GOLD AND UNAFFECTED CUSTOMERS
# =============================================================================

gold_df = spark.table(GOLD_VIEW)
assert_equal(
    gold_df.filter(col("customer_id") == TEST_CUSTOMER_ID).count(),
    0,
    "C002 Gold rows after deletion",
)
assert_equal(
    collect_values(gold_df, "customer_id"),
    EXPECTED_ACTIVE_CUSTOMER_IDS,
    "Gold customer IDs after deletion",
)
assert_equal(
    collect_values(
        spark.table(SILVER_TABLE).filter(col("__END_AT").isNull()),
        "customer_id",
    ),
    EXPECTED_ACTIVE_CUSTOMER_IDS,
    "Active Satellite customer IDs after deletion",
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2.1 DELETE-PROPAGATION ASSERTIONS PASSED")
print("=" * 80)
print(f"Customer:                {TEST_CUSTOMER_ID}")
print(f"Customer hash key:       {expected_c002_hk}")
print(f"UPDATE commit version:   {update_version}")
print(f"DELETE commit version:   {delete_version}")
print("Hub C002 rows:           1")
print("Satellite history rows:  2")
print("Satellite active rows:   0")
print("Deletion markers:        1")
print("Delete-classified rows:  1")
print("Gold C002 rows:          0")
print()
print("Conclusion:")
print(
    "The update closed one Satellite version without a deletion marker. "
    "The later source delete closed the active version, created no "
    "replacement, preserved the Hub identity, created one durable marker, "
    "and removed C002 from Gold."
)
print()
print("Replay/idempotency check:")
print("1. Run Pipeline 2 normally again with no new input.")
print("2. Rerun this script; the marker count must remain one.")
print("=" * 80)

classified_history.orderBy("__START_AT").show(truncate=False)

