"""
Validate POC 2.1 after both pipelines complete their initial FULL REFRESH.

This script proves the starting state before the update/delete classification
test. It intentionally verifies only that CDF is enabled; it does not assume
CDF was recorded from Delta version zero.
"""

import hashlib
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.poc21_bronze_customers"
DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc21_dqx_customers_valid"
DQX_QUARANTINE_TABLE = (
    f"{CATALOG}.{SCHEMA}.poc21_dqx_customers_quarantine"
)
HUB_TABLE = f"{CATALOG}.{SCHEMA}.poc21_hub_customers"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.poc21_satellite_customers_scd2"
MARKER_TABLE = f"{CATALOG}.{SCHEMA}.poc21_customer_deletion_markers"
CLASSIFIED_VIEW = (
    f"{CATALOG}.{SCHEMA}.poc21_satellite_customers_classified"
)
GOLD_VIEW = f"{CATALOG}.{SCHEMA}.poc21_gold_customers_current"

TEST_CUSTOMER_ID = "C002"

EXPECTED_VALID_CUSTOMER_IDS = {
    "C001",
    "C002",
    "C004",
}
EXPECTED_QUARANTINE_NAMES = {
    "Charlie Brown",
    "Missing Identifier",
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


def get_table_properties(
    table_name: str,
) -> dict[str, str]:
    return {
        row["key"]: row["value"]
        for row in spark.sql(
            f"SHOW TBLPROPERTIES {table_name}"
        ).collect()
    }


def expected_customer_hash_key(customer_id: str) -> str:
    normalized_value = f"CUSTOMER||{customer_id.strip().upper()}"
    return hashlib.sha256(normalized_value.encode("utf-8")).hexdigest()


# =============================================================================
# LOAD STATE
# =============================================================================

bronze_df = spark.table(BRONZE_TABLE)
valid_df = spark.table(DQX_VALID_TABLE)
quarantine_df = spark.table(DQX_QUARANTINE_TABLE)
hub_df = spark.table(HUB_TABLE)
silver_df = spark.table(SILVER_TABLE)
marker_df = spark.table(MARKER_TABLE)
classified_df = spark.table(CLASSIFIED_VIEW)
gold_df = spark.table(GOLD_VIEW)


# =============================================================================
# BRONZE AND DQX
# =============================================================================

assert_equal(bronze_df.count(), 5, "Bronze row count")
assert_equal(valid_df.count(), 3, "DQX-valid row count")
assert_equal(quarantine_df.count(), 2, "DQX-quarantine row count")

assert_equal(
    collect_values(valid_df, "customer_id"),
    EXPECTED_VALID_CUSTOMER_IDS,
    "DQX-valid customer IDs",
)
assert_equal(
    collect_values(quarantine_df, "name"),
    EXPECTED_QUARANTINE_NAMES,
    "DQX-quarantine names",
)

invalid_valid_row_count = (
    valid_df
    .filter(
        ~(
            col("customer_id_passed")
            & col("email_passed")
            & col("updated_at_passed")
        )
    )
    .count()
)
assert_equal(
    invalid_valid_row_count,
    0,
    "DQX-valid rows failing an explicit quality flag",
)

charlie_row = (
    quarantine_df
    .filter(col("name") == "Charlie Brown")
    .select(
        "customer_id_passed",
        "email_passed",
        "updated_at_passed",
    )
    .first()
)
assert charlie_row is not None, "Expected Charlie Brown in quarantine."
assert_equal(
    tuple(charlie_row),
    (True, False, True),
    "Charlie Brown DQX flags",
)

missing_id_row = (
    quarantine_df
    .filter(col("name") == "Missing Identifier")
    .select(
        "customer_id_passed",
        "email_passed",
        "updated_at_passed",
    )
    .first()
)
assert missing_id_row is not None, "Expected Missing Identifier in quarantine."
assert_equal(
    tuple(missing_id_row),
    (False, True, True),
    "Missing Identifier DQX flags",
)


# =============================================================================
# RETENTION TIMESTAMP CONTRACT
# =============================================================================

for table_name, dataframe in [
    (BRONZE_TABLE, bronze_df),
    (DQX_VALID_TABLE, valid_df),
    (DQX_QUARANTINE_TABLE, quarantine_df),
]:
    assert_equal(
        dataframe.filter(col("_ingested_at").isNull()).count(),
        0,
        f"Null _ingested_at count in {table_name}",
    )

bronze_ingestion_timestamp_by_name = {
    row["name"]: row["_ingested_at"]
    for row in bronze_df.select("name", "_ingested_at").collect()
}

for dataframe, route_name in [
    (valid_df, "valid"),
    (quarantine_df, "quarantine"),
]:
    for row in dataframe.select("name", "_ingested_at").collect():
        assert_equal(
            row["_ingested_at"],
            bronze_ingestion_timestamp_by_name[row["name"]],
            f"Preserved _ingested_at for {route_name} row {row['name']}",
        )


# =============================================================================
# CDF CONFIGURATION
# =============================================================================

valid_properties = get_table_properties(DQX_VALID_TABLE)
assert_equal(
    valid_properties.get("delta.enableChangeDataFeed", "").lower(),
    "true",
    "Change Data Feed property on DQX-valid table",
)


# =============================================================================
# INITIAL HUB, SATELLITE, MARKER, CLASSIFICATION, AND GOLD
# =============================================================================

expected_c002_hk = expected_customer_hash_key(TEST_CUSTOMER_ID)

assert_equal(hub_df.count(), 3, "Initial Hub row count")
assert_equal(
    hub_df.select("customer_hk").distinct().count(),
    3,
    "Initial distinct Hub hash-key count",
)

c002_hub = (
    hub_df
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .select(
        "customer_hk",
        "customer_id",
        "record_source",
        "hub_loaded_at",
    )
    .first()
)
assert c002_hub is not None, "Expected an initial C002 Hub row."
assert_equal(
    c002_hub["customer_hk"],
    expected_c002_hk,
    "Deterministic C002 Hub hash key",
)
assert_equal(
    c002_hub["record_source"],
    "POC21_DQX_VALID",
    "C002 Hub record source",
)
assert c002_hub["hub_loaded_at"] is not None

assert_equal(silver_df.count(), 3, "Initial Silver history row count")
assert_equal(
    silver_df.filter(col("__END_AT").isNull()).count(),
    3,
    "Initial active Silver row count",
)
assert_equal(
    silver_df.filter(col("__END_AT").isNotNull()).count(),
    0,
    "Initial closed Silver row count",
)
assert_equal(marker_df.count(), 0, "Initial deletion-marker row count")
assert_equal(classified_df.count(), 3, "Initial classified Silver row count")
assert_equal(
    classified_df.filter(col("closed_by_delete")).count(),
    0,
    "Initially delete-classified Silver rows",
)
assert_equal(gold_df.count(), 3, "Initial Gold row count")
assert_equal(
    collect_values(gold_df, "customer_id"),
    EXPECTED_VALID_CUSTOMER_IDS,
    "Initial Gold customer IDs",
)

c002_classified = (
    classified_df
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .select(
        "customer_hk",
        "email",
        "city",
        "__START_AT",
        "__END_AT",
        "closed_by_delete",
        "delete_sequence",
        "deletion_reason",
        "deletion_request_id",
    )
    .first()
)

assert c002_classified is not None, "Expected initial classified C002 row."
assert_equal(
    c002_classified["customer_hk"],
    expected_c002_hk,
    "Initial classified C002 hash key",
)
assert_equal(
    c002_classified["email"],
    "bob@example.com",
    "Initial C002 email",
)
assert_equal(
    c002_classified["city"],
    "Los Angeles",
    "Initial C002 city",
)
assert c002_classified["__START_AT"] is not None
assert c002_classified["__END_AT"] is None
assert_equal(
    c002_classified["closed_by_delete"],
    False,
    "Initial C002 closed_by_delete",
)
assert c002_classified["delete_sequence"] is None
assert c002_classified["deletion_reason"] is None
assert c002_classified["deletion_request_id"] is None

c002_gold = (
    gold_df
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .select("customer_hk")
    .first()
)
assert c002_gold is not None, "Expected an initial C002 Gold row."
assert_equal(
    c002_gold["customer_hk"],
    expected_c002_hk,
    "Initial Gold C002 hash key",
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2.1 INITIAL-STATE ASSERTIONS PASSED")
print("=" * 80)
print("Bronze rows:             5")
print("DQX-valid rows:          3")
print("DQX-quarantine rows:     2")
print("Hub rows:                3")
print("Silver history rows:     3")
print("Deletion markers:        0")
print("Delete-classified rows:  0")
print("Gold rows:               3")
print()
print("Next: run 05_update_dqx_valid_row.py.")
print("=" * 80)
