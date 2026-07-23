"""
Deterministic assertions after:
    1. Retention pipeline FULL REFRESH succeeds.
    2. CDC/Gold pipeline FULL REFRESH succeeds.

This verifies:
    Auto Loader ingestion
    DQX valid/quarantine routing
    _ingested_at preservation
    independent Auto-TTL policies
    DQX-valid legacy CDF
    initial SCD2 state
    initial Gold state
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

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_bronze_customers"
DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
DQX_QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_quarantine"
SATELLITE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_satellite_customers_scd2"
GOLD_VIEW = f"{CATALOG}.{SCHEMA}.poc2_gold_customers_current"

EXPECTED_VALID_IDS = {"C001", "C002", "C004"}
EXPECTED_QUARANTINE_NAMES = {"Charlie Brown", "Missing Identifier"}

EXPECTED_TTL = {
    BRONZE_TABLE: ("30", "_ingested_at"),
    DQX_VALID_TABLE: ("20", "_ingested_at"),
    DQX_QUARANTINE_TABLE: ("10", "_ingested_at"),
}


# =============================================================================
# HELPERS
# =============================================================================

def assert_equal(actual: Any, expected: Any, description: str) -> None:
    assert actual == expected, (
        f"{description}: expected {expected!r}, found {actual!r}."
    )


def table_properties(table_name: str) -> dict[str, str]:
    return {
        row["key"]: row["value"]
        for row in spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    }


# =============================================================================
# LOAD TABLES
# =============================================================================

bronze_df = spark.table(BRONZE_TABLE)
valid_df = spark.table(DQX_VALID_TABLE)
quarantine_df = spark.table(DQX_QUARANTINE_TABLE)
satellite_df = spark.table(SATELLITE_TABLE)
gold_df = spark.table(GOLD_VIEW)


# =============================================================================
# INGESTION AND ROUTING
# =============================================================================

assert_equal(bronze_df.count(), 5, "Bronze row count")
assert_equal(valid_df.count(), 3, "DQX-valid row count")
assert_equal(quarantine_df.count(), 2, "DQX-quarantine row count")

actual_valid_ids = {
    row["customer_id"]
    for row in valid_df.select("customer_id").collect()
}
assert_equal(actual_valid_ids, EXPECTED_VALID_IDS, "DQX-valid customer IDs")

actual_quarantine_names = {
    row["name"]
    for row in quarantine_df.select("name").collect()
}
assert_equal(
    actual_quarantine_names,
    EXPECTED_QUARANTINE_NAMES,
    "DQX-quarantine names",
)


# =============================================================================
# QUALITY FLAGS AND DQX RESULTS
# =============================================================================

assert valid_df.filter(
    ~(
        col("customer_id_passed")
        & col("email_passed")
        & col("updated_at_passed")
    )
).count() == 0, "Every DQX-valid row must pass all three explicit checks."

invalid_email = quarantine_df.filter(col("name") == "Charlie Brown").first()
assert invalid_email is not None, "Missing Charlie Brown quarantine row."
assert invalid_email["customer_id_passed"] is True
assert invalid_email["email_passed"] is False
assert invalid_email["updated_at_passed"] is True

missing_id = quarantine_df.filter(col("name") == "Missing Identifier").first()
assert missing_id is not None, "Missing missing-identifier quarantine row."
assert missing_id["customer_id_passed"] is False
assert missing_id["email_passed"] is True
assert missing_id["updated_at_passed"] is True


# =============================================================================
# _INGESTED_AT PRESERVATION
# =============================================================================

for table_name, df in [
    (BRONZE_TABLE, bronze_df),
    (DQX_VALID_TABLE, valid_df),
    (DQX_QUARANTINE_TABLE, quarantine_df),
]:
    null_count = df.filter(col("_ingested_at").isNull()).count()
    assert_equal(null_count, 0, f"Null _ingested_at count in {table_name}")

bronze_ingestion_by_name = {
    row["name"]: row["_ingested_at"]
    for row in bronze_df.select("name", "_ingested_at").collect()
}

for row in valid_df.select("name", "_ingested_at").collect():
    assert_equal(
        row["_ingested_at"],
        bronze_ingestion_by_name[row["name"]],
        f"Preserved _ingested_at for valid row {row['name']}",
    )

for row in quarantine_df.select("name", "_ingested_at").collect():
    assert_equal(
        row["_ingested_at"],
        bronze_ingestion_by_name[row["name"]],
        f"Preserved _ingested_at for quarantine row {row['name']}",
    )


# =============================================================================
# AUTO-TTL POLICY CONFIGURATION
# =============================================================================

for table_name, (expected_days, expected_column) in EXPECTED_TTL.items():
    properties = table_properties(table_name)
    assert_equal(
        properties.get("autottl.expireInDays"),
        expected_days,
        f"Auto-TTL expiration days for {table_name}",
    )
    assert_equal(
        properties.get("autottl.timestampColumn"),
        expected_column,
        f"Auto-TTL timestamp column for {table_name}",
    )


# =============================================================================
# LEGACY CDF CONFIGURATION AND INITIAL EVENTS
# =============================================================================

valid_properties = table_properties(DQX_VALID_TABLE)
assert_equal(
    valid_properties.get("delta.enableChangeDataFeed", "").lower(),
    "true",
    "Legacy CDF property on DQX-valid table",
)

initial_cdf = spark.sql(
    f"""
    SELECT customer_id, _change_type
    FROM table_changes('{DQX_VALID_TABLE}', 0)
    """
)

assert_equal(initial_cdf.count(), 3, "Initial DQX-valid CDF row count")

assert_equal(
    {
        row["_change_type"]
        for row in initial_cdf.select("_change_type").distinct().collect()
    },
    {"insert"},
    "Initial DQX-valid CDF change types",
)


# =============================================================================
# INITIAL SCD2 AND GOLD STATE
# =============================================================================

assert_equal(satellite_df.count(), 3, "Initial satellite history row count")
assert_equal(
    satellite_df.filter(col("__END_AT").isNull()).count(),
    3,
    "Initial active satellite row count",
)
assert_equal(
    satellite_df.filter(col("__END_AT").isNotNull()).count(),
    0,
    "Initial closed satellite row count",
)

assert_equal(gold_df.count(), 3, "Initial Gold row count")
assert_equal(
    {
        row["customer_id"]
        for row in gold_df.select("customer_id").collect()
    },
    EXPECTED_VALID_IDS,
    "Initial Gold customer IDs",
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2 INITIAL-STATE ASSERTIONS PASSED")
print("=" * 80)
print("Bronze rows:             5")
print("DQX-valid rows:          3")
print("DQX-quarantine rows:     2")
print("Active satellite rows:   3")
print("Gold rows:               3")
print()
print("Verified:")
print("- Auto Loader ingestion")
print("- DQX routing and detailed quality flags")
print("- Preserved _ingested_at")
print("- Independent Bronze/valid/quarantine Auto-TTL policies")
print("- Legacy CDF on DQX-valid")
print("- Initial AUTO CDC SCD2 and Gold state")
print()
print("Next step:")
print("Run 05_delete_dqx_valid_row.py.")
print("=" * 80)
