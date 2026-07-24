"""
Display non-destructive POC 2.1 evidence.

This script is safe to rerun before or after the update/delete phases. CDF is
queried only at exact UPDATE and DELETE commit versions found in Delta
history; the script never assumes CDF was recorded from version zero.
"""

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, when


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


# =============================================================================
# HELPERS
# =============================================================================

def show_section(title: str) -> None:
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)


def find_latest_operation_commit(
    table_name: str,
    operation: str,
) -> Row | None:
    return (
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


def show_exact_cdf_version(
    title: str,
    commit: Row | None,
) -> None:
    show_section(title)

    if commit is None:
        print("No matching Delta operation exists yet.")
        return

    version = int(commit["version"])
    print(f"Delta version: {version}")
    print(f"Timestamp:     {commit['timestamp']}")
    print(f"Metrics:       {commit['operationMetrics'] or {}}")
    print()

    (
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
            FROM table_changes(
                '{DQX_VALID_TABLE}',
                {version},
                {version}
            )
            WHERE customer_id = '{TEST_CUSTOMER_ID}'
            ORDER BY _change_type
            """
        )
        .show(truncate=False)
    )


# =============================================================================
# CURRENT INGESTION AND STAGING STATE
# =============================================================================

show_section("BRONZE CURRENT STATE")
spark.table(BRONZE_TABLE).orderBy("customer_id", "name").show(truncate=False)

show_section("DQX-VALID STAGING CURRENT STATE")
spark.table(DQX_VALID_TABLE).orderBy("customer_id").show(truncate=False)

show_section("DQX-QUARANTINE CURRENT STATE")
spark.table(DQX_QUARANTINE_TABLE).orderBy("name").show(truncate=False)


# =============================================================================
# EXACT SOURCE CDF EVIDENCE
# =============================================================================

update_commit = find_latest_operation_commit(DQX_VALID_TABLE, "UPDATE")
delete_commit = find_latest_operation_commit(DQX_VALID_TABLE, "DELETE")

show_exact_cdf_version(
    f"EXACT DQX UPDATE CDF FOR {TEST_CUSTOMER_ID}",
    update_commit,
)
show_exact_cdf_version(
    f"EXACT DQX DELETE CDF FOR {TEST_CUSTOMER_ID}",
    delete_commit,
)


# =============================================================================
# DATA VAULT AND SERVING STATE
# =============================================================================

show_section("CUSTOMER HUB")
(
    spark.table(HUB_TABLE)
    .orderBy("customer_id")
    .show(truncate=False)
)

show_section("CUSTOMER SCD TYPE 2 SATELLITE")
(
    spark.table(SILVER_TABLE)
    .withColumn(
        "record_status",
        when(col("__END_AT").isNull(), "ACTIVE").otherwise("CLOSED"),
    )
    .orderBy("customer_id", "__START_AT")
    .show(truncate=False)
)

show_section("DURABLE CUSTOMER DELETION MARKERS")
(
    spark.table(MARKER_TABLE)
    .orderBy("customer_id", "delete_sequence")
    .show(truncate=False)
)

show_section("CLASSIFIED SATELLITE HISTORY")
(
    spark.table(CLASSIFIED_VIEW)
    .orderBy("customer_id", "__START_AT")
    .show(truncate=False)
)

show_section("GOLD CURRENT STATE")
(
    spark.table(GOLD_VIEW)
    .orderBy("customer_id")
    .show(truncate=False)
)


# =============================================================================
# DELTA HISTORY
# =============================================================================

for title, table_name in [
    ("DQX-VALID DELTA HISTORY", DQX_VALID_TABLE),
    ("HUB DELTA HISTORY", HUB_TABLE),
    ("SATELLITE DELTA HISTORY", SILVER_TABLE),
    ("MARKER DELTA HISTORY", MARKER_TABLE),
]:
    show_section(title)
    (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
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


# =============================================================================
# SUMMARY
# =============================================================================

show_section("POC 2.1 EVIDENCE COMPLETE")

if update_commit is None:
    print("UPDATE phase not found.")
else:
    print(f"UPDATE version: {int(update_commit['version'])}")

if delete_commit is None:
    print("DELETE phase not found.")
else:
    print(f"DELETE version: {int(delete_commit['version'])}")

print(
    "The Hub preserves customer identity. The Satellite preserves descriptive "
    "history. The marker identifies actual delete closure. Gold exposes only "
    "customers with an active Satellite version."
)

