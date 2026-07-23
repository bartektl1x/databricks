"""
Deterministically generates the DQX-valid CDF DELETE used by POC 2.

Why explicit DELETE:
    Auto-TTL execution is asynchronous and has no guaranteed run time.
    This command deterministically produces the same relevant Delta CDF delete
    event that a DQX-valid Auto-TTL DELETE operation produces.

After this script succeeds:
    1. Run a NORMAL INCREMENTAL update of the CDC/Gold pipeline.
    2. Run 06_assert_delete_propagation.py.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "dev_mr_dhc_bronze"
SCHEMA = "slpat_landing_staging"

DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
TEST_CUSTOMER_ID = "C002"


# =============================================================================
# PRECONDITION
# =============================================================================

before_count = (
    spark.table(DQX_VALID_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .count()
)

assert before_count == 1, (
    f"Expected exactly one DQX-valid row for {TEST_CUSTOMER_ID}, "
    f"but found {before_count}. Reset POC 2 and rerun both full refreshes."
)


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
# VERIFY CURRENT STATE AND CDF
# =============================================================================

after_count = (
    spark.table(DQX_VALID_TABLE)
    .filter(col("customer_id") == TEST_CUSTOMER_ID)
    .count()
)

assert after_count == 0, (
    f"Expected no DQX-valid row for {TEST_CUSTOMER_ID} after DELETE, "
    f"but found {after_count}."
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
    FROM table_changes('{DQX_VALID_TABLE}', 0)
    WHERE customer_id = '{TEST_CUSTOMER_ID}'
      AND _change_type = 'delete'
    """
)

assert delete_events.count() == 1, (
    f"Expected exactly one CDF delete event for {TEST_CUSTOMER_ID}."
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2 DQX-VALID DELETE CREATED")
print("=" * 80)
print(f"Customer:          {TEST_CUSTOMER_ID}")
print("DQX-valid rows:    0")
print("CDF delete events: 1")
print()
print("Next steps:")
print("1. Run a NORMAL INCREMENTAL update of the CDC/Gold pipeline.")
print("2. Run 06_assert_delete_propagation.py.")
print("=" * 80)

delete_events.show(truncate=False)
