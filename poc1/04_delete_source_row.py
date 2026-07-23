"""
04_delete_source_row.py

Deletes C002 from the Delta source table.

After this script succeeds:
    1. Run a NORMAL INCREMENTAL pipeline update.
    2. Wait for that pipeline update to complete successfully.
    3. Run 05_assert_delete_propagation.py.

This script intentionally fails if C002 is already absent, preventing an
accidental rerun from hiding an invalid test state.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.source_customers"
TEST_CUSTOMER_ID = "C002"


# =============================================================================
# PRECONDITION
# =============================================================================

before_count = (
    spark.table(SOURCE_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .count()
)

assert before_count == 1, (
    f"Expected exactly one source row for {TEST_CUSTOMER_ID} before deletion, "
    f"but found {before_count}. "
    "Reset the POC by rerunning 01_setup_source.py followed by a full pipeline "
    "refresh."
)


# =============================================================================
# DELETE SOURCE RECORD
# =============================================================================

spark.sql(
    f"""
    DELETE FROM {SOURCE_TABLE}
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
    """
)


# =============================================================================
# VERIFY SOURCE DELETE
# =============================================================================

after_count = (
    spark.table(SOURCE_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .count()
)

assert after_count == 0, (
    f"Source deletion failed. Found {after_count} remaining source rows for "
    f"{TEST_CUSTOMER_ID}."
)


# =============================================================================
# VERIFY CDF DELETE EVENT
# =============================================================================

delete_events = spark.sql(
    f"""
    SELECT
        customer_id,
        name,
        email,
        city,
        updated_at,
        _change_type,
        _commit_version,
        _commit_timestamp
    FROM table_changes('{SOURCE_TABLE}', 0)
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
      AND _change_type = 'delete'
    """
)

delete_event_count = delete_events.count()

assert delete_event_count == 1, (
    f"Expected exactly one CDF delete event for {TEST_CUSTOMER_ID}, "
    f"but found {delete_event_count}."
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("SOURCE DELETE COMPLETED")
print("=" * 80)
print(f"Customer:          {TEST_CUSTOMER_ID}")
print("Source rows:       0")
print("CDF delete events: 1")
print()
print("Next steps:")
print("1. Run a NORMAL INCREMENTAL pipeline update.")
print("2. Wait for it to complete successfully.")
print("3. Run 05_assert_delete_propagation.py.")
print("=" * 80)

delete_events.orderBy("_commit_version").show(truncate=False)
