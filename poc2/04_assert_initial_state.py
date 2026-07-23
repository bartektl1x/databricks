"""
Validates the deterministic state after both POC 2 pipeline FULL REFRESH
operations complete successfully.

Expected state:
    Bronze rows:          5
    DQX-valid rows:       3
    DQX-quarantine rows:  2
    Silver history rows:  3
    Silver active rows:   3
    Silver closed rows:   0
    Gold rows:            3

This script verifies that Change Data Feed is enabled on the DQX-valid table.
It intentionally does not query table_changes(..., 0), because CDF might have
been enabled after the table's initial Delta version.
"""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_bronze_customers"
DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
DQX_QUARANTINE_TABLE = (
    f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_quarantine"
)
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.poc2_satellite_customers_scd2"
GOLD_VIEW = f"{CATALOG}.{SCHEMA}.poc2_gold_customers_current"

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


def get_table_properties(
    table_name: str,
) -> dict[str, str]:
    return {
        row["key"]: row["value"]
        for row in spark.sql(
            f"SHOW TBLPROPERTIES {table_name}"
        ).collect()
    }


# =============================================================================
# LOAD CURRENT STATE
# =============================================================================

bronze_df = spark.table(BRONZE_TABLE)
valid_df = spark.table(DQX_VALID_TABLE)
quarantine_df = spark.table(DQX_QUARANTINE_TABLE)
silver_df = spark.table(SILVER_TABLE)
gold_df = spark.table(GOLD_VIEW)


# =============================================================================
# ASSERT BRONZE AND DQX ROW COUNTS
# =============================================================================

assert_equal(
    bronze_df.count(),
    5,
    "Bronze row count",
)

assert_equal(
    valid_df.count(),
    3,
    "DQX-valid row count",
)

assert_equal(
    quarantine_df.count(),
    2,
    "DQX-quarantine row count",
)


# =============================================================================
# ASSERT DQX ROUTING
# =============================================================================

actual_valid_customer_ids = collect_values(
    valid_df,
    "customer_id",
)

assert_equal(
    actual_valid_customer_ids,
    EXPECTED_VALID_CUSTOMER_IDS,
    "DQX-valid customer IDs",
)

actual_quarantine_names = collect_values(
    quarantine_df,
    "name",
)

assert_equal(
    actual_quarantine_names,
    EXPECTED_QUARANTINE_NAMES,
    "DQX-quarantine names",
)


# =============================================================================
# ASSERT VALID-ROW QUALITY FLAGS
# =============================================================================

invalid_flag_count = (
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
    invalid_flag_count,
    0,
    "DQX-valid rows failing an explicit quality flag",
)


# =============================================================================
# ASSERT QUARANTINE REASONS
# =============================================================================

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

assert charlie_row is not None, (
    "Expected Charlie Brown in the DQX quarantine table."
)

assert_equal(
    charlie_row["customer_id_passed"],
    True,
    "Charlie Brown customer_id quality flag",
)

assert_equal(
    charlie_row["email_passed"],
    False,
    "Charlie Brown email quality flag",
)

assert_equal(
    charlie_row["updated_at_passed"],
    True,
    "Charlie Brown updated_at quality flag",
)


missing_identifier_row = (
    quarantine_df
    .filter(col("name") == "Missing Identifier")
    .select(
        "customer_id_passed",
        "email_passed",
        "updated_at_passed",
    )
    .first()
)

assert missing_identifier_row is not None, (
    "Expected Missing Identifier in the DQX quarantine table."
)

assert_equal(
    missing_identifier_row["customer_id_passed"],
    False,
    "Missing Identifier customer_id quality flag",
)

assert_equal(
    missing_identifier_row["email_passed"],
    True,
    "Missing Identifier email quality flag",
)

assert_equal(
    missing_identifier_row["updated_at_passed"],
    True,
    "Missing Identifier updated_at quality flag",
)


# =============================================================================
# ASSERT _INGESTED_AT IS POPULATED
# =============================================================================

for table_name, dataframe in [
    (BRONZE_TABLE, bronze_df),
    (DQX_VALID_TABLE, valid_df),
    (DQX_QUARANTINE_TABLE, quarantine_df),
]:
    null_ingestion_timestamp_count = (
        dataframe
        .filter(col("_ingested_at").isNull())
        .count()
    )

    assert_equal(
        null_ingestion_timestamp_count,
        0,
        f"Null _ingested_at count in {table_name}",
    )


# =============================================================================
# ASSERT _INGESTED_AT IS PRESERVED FROM BRONZE
# =============================================================================

bronze_ingestion_timestamp_by_name = {
    row["name"]: row["_ingested_at"]
    for row in (
        bronze_df
        .select(
            "name",
            "_ingested_at",
        )
        .collect()
    )
}

for row in (
    valid_df
    .select(
        "name",
        "_ingested_at",
    )
    .collect()
):
    assert_equal(
        row["_ingested_at"],
        bronze_ingestion_timestamp_by_name[row["name"]],
        f"Preserved _ingested_at for valid row {row['name']}",
    )

for row in (
    quarantine_df
    .select(
        "name",
        "_ingested_at",
    )
    .collect()
):
    assert_equal(
        row["_ingested_at"],
        bronze_ingestion_timestamp_by_name[row["name"]],
        f"Preserved _ingested_at for quarantine row {row['name']}",
    )


# =============================================================================
# ASSERT LEGACY CHANGE DATA FEED CONFIGURATION
# =============================================================================

valid_table_properties = get_table_properties(
    DQX_VALID_TABLE
)

cdf_enabled = valid_table_properties.get(
    "delta.enableChangeDataFeed",
    "",
).lower()

assert_equal(
    cdf_enabled,
    "true",
    "Change Data Feed property on DQX-valid table",
)

# Do not call table_changes(DQX_VALID_TABLE, 0) here.
#
# Pipeline-managed tables can have Delta versions created before the CDF
# property becomes active. CDF is not retroactive, so querying from version 0
# can fail even though CDF is currently enabled and working correctly.
#
# The DELETE test captures and queries the exact DELETE commit version instead.


# =============================================================================
# ASSERT INITIAL SILVER SCD2 STATE
# =============================================================================

assert_equal(
    silver_df.count(),
    3,
    "Initial Silver history row count",
)

assert_equal(
    silver_df
    .filter(col("__END_AT").isNull())
    .count(),
    3,
    "Initial active Silver row count",
)

assert_equal(
    silver_df
    .filter(col("__END_AT").isNotNull())
    .count(),
    0,
    "Initial closed Silver row count",
)

actual_active_silver_customer_ids = collect_values(
    silver_df.filter(col("__END_AT").isNull()),
    "customer_id",
)

assert_equal(
    actual_active_silver_customer_ids,
    EXPECTED_VALID_CUSTOMER_IDS,
    "Initial active Silver customer IDs",
)


# =============================================================================
# ASSERT INITIAL GOLD STATE
# =============================================================================

assert_equal(
    gold_df.count(),
    3,
    "Initial Gold row count",
)

actual_gold_customer_ids = collect_values(
    gold_df,
    "customer_id",
)

assert_equal(
    actual_gold_customer_ids,
    EXPECTED_VALID_CUSTOMER_IDS,
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
print("Silver history rows:     3")
print("Silver active rows:      3")
print("Silver closed rows:      0")
print("Gold rows:               3")
print()
print("Verified:")
print("- Auto Loader Bronze ingestion")
print("- DQX valid and quarantine routing")
print("- Explicit DQ quality flags")
print("- Preserved _ingested_at values")
print("- Change Data Feed enabled on DQX valid")
print("- Initial AUTO CDC SCD2 state")
print("- Initial Gold current state")
print()
print("Next step:")
print("Run 05_delete_dqx_valid_row.py.")
print("=" * 80)
