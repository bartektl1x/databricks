"""
Displays non-destructive evidence for POC 2.

Safe to rerun.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

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

TEST_CUSTOMER_ID = "C002"


def show_title(title: str) -> None:
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)


show_title("BRONZE CURRENT STATE")
spark.table(BRONZE_TABLE).orderBy("name").show(truncate=False)

show_title("DQX VALID CURRENT STATE")
spark.table(DQX_VALID_TABLE).orderBy("customer_id").show(truncate=False)

show_title("DQX QUARANTINE CURRENT STATE")
spark.table(DQX_QUARANTINE_TABLE).orderBy("name").show(truncate=False)

show_title("AUTO-TTL TABLE PROPERTIES")
for table_name in [
    BRONZE_TABLE,
    DQX_VALID_TABLE,
    DQX_QUARANTINE_TABLE,
]:
    print()
    print(table_name)
    (
        spark.sql(f"SHOW TBLPROPERTIES {table_name}")
        .filter(
            col("key").isin(
                "autottl.expireInDays",
                "autottl.timestampColumn",
                "delta.enableChangeDataFeed",
            )
        )
        .orderBy("key")
        .show(truncate=False)
    )

show_title(f"DQX VALID CDF EVENTS FOR {TEST_CUSTOMER_ID}")
spark.sql(
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
    ORDER BY _commit_version, _change_type
    """
).show(truncate=False)

show_title("FINAL SCD2 SATELLITE")
(
    spark.table(SATELLITE_TABLE)
    .withColumn(
        "record_status",
        when(col("__END_AT").isNull(), "ACTIVE").otherwise("CLOSED"),
    )
    .orderBy("customer_id", "__START_AT")
    .show(truncate=False)
)

show_title("GOLD CURRENT STATE")
spark.table(GOLD_VIEW).orderBy("customer_id").show(truncate=False)

show_title("DQX VALID DELTA HISTORY")
(
    spark.sql(f"DESCRIBE HISTORY {DQX_VALID_TABLE}")
    .select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "operationMetrics",
    )
    .orderBy(col("version").asc())
    .show(truncate=False)
)

show_title("SATELLITE DELTA HISTORY")
(
    spark.sql(f"DESCRIBE HISTORY {SATELLITE_TABLE}")
    .select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "operationMetrics",
    )
    .orderBy(col("version").asc())
    .show(truncate=False)
)

show_title("PREDICTIVE OPTIMIZATION OPERATIONS FOR POC 2 TABLES")
spark.sql(
    f"""
    SELECT
        catalog_name,
        schema_name,
        table_name,
        operation_type,
        start_time,
        end_time,
        status,
        usage_quantity,
        usage_unit
    FROM system.storage.predictive_optimization_operations_history
    WHERE catalog_name = '{CATALOG}'
      AND schema_name = '{SCHEMA}'
      AND table_name IN (
          'poc2_bronze_customers',
          'poc2_dqx_customers_valid',
          'poc2_dqx_customers_quarantine'
      )
    ORDER BY start_time DESC
    """
).show(truncate=False)
