"""
Displays non-destructive evidence for POC 2.

This script is safe to rerun after the DELETE propagation test completes.

Displayed evidence:
    - Bronze current state
    - DQX-valid current state
    - DQX-quarantine current state
    - Exact CDF DELETE event for C002
    - Silver SCD Type 2 state
    - Gold current state
    - DQX-valid Delta history
    - Silver Delta history

The CDF query intentionally uses the exact DELETE commit version rather than
version 0 because Change Data Feed might have been enabled after the table's
initial Delta version.
"""

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, when


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

TEST_CUSTOMER_ID = "C002"


# =============================================================================
# HELPERS
# =============================================================================

def show_section(title: str) -> None:
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)


def find_latest_delete_commit(
    table_name: str,
) -> Row | None:
    """
    Return the latest Delta DELETE commit, or None when no DELETE exists.

    Returning None makes the evidence script usable both before and after the
    deterministic DELETE test.
    """

    return (
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


# =============================================================================
# BRONZE CURRENT STATE
# =============================================================================

show_section("BRONZE CURRENT STATE")

(
    spark.table(BRONZE_TABLE)
    .select(
        "customer_id",
        "name",
        "email",
        "city",
        "updated_at",
        "_updated_at_raw",
        "_ingested_at",
        "_source_file",
        "_source_file_name",
    )
    .orderBy(
        "customer_id",
        "name",
    )
    .show(
        truncate=False,
    )
)


# =============================================================================
# DQX-VALID CURRENT STATE
# =============================================================================

show_section("DQX-VALID CURRENT STATE")

(
    spark.table(DQX_VALID_TABLE)
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
    )
    .orderBy("customer_id")
    .show(
        truncate=False,
    )
)


# =============================================================================
# DQX-QUARANTINE CURRENT STATE
# =============================================================================

show_section("DQX-QUARANTINE CURRENT STATE")

(
    spark.table(DQX_QUARANTINE_TABLE)
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
        "_errors",
        "_warnings",
    )
    .orderBy("name")
    .show(
        truncate=False,
    )
)


# =============================================================================
# EXACT DQX-VALID CDF DELETE EVENT
# =============================================================================

show_section(
    f"DQX-VALID CDF DELETE EVENT FOR {TEST_CUSTOMER_ID}"
)

delete_commit = find_latest_delete_commit(
    DQX_VALID_TABLE
)

if delete_commit is None:
    print(
        "No DELETE commit exists yet. "
        "Run 05_delete_dqx_valid_row.py before expecting CDF delete evidence."
    )
else:
    delete_commit_version = int(
        delete_commit["version"]
    )

    operation_metrics = (
        delete_commit["operationMetrics"]
        or {}
    )

    print(f"DELETE commit version: {delete_commit_version}")
    print(f"DELETE timestamp:      {delete_commit['timestamp']}")
    print(f"Operation metrics:     {operation_metrics}")
    print()

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
        ORDER BY
            _commit_version,
            _change_type
        """
    )

    delete_events.show(
        truncate=False,
    )


# =============================================================================
# SILVER SCD TYPE 2 STATE
# =============================================================================

show_section("SILVER SCD TYPE 2 STATE")

(
    spark.table(SILVER_TABLE)
    .withColumn(
        "record_status",
        when(
            col("__END_AT").isNull(),
            "ACTIVE",
        ).otherwise("CLOSED"),
    )
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
        "record_status",
    )
    .orderBy(
        "customer_id",
        "__START_AT",
    )
    .show(
        truncate=False,
    )
)


# =============================================================================
# GOLD CURRENT STATE
# =============================================================================

show_section("GOLD CURRENT STATE")

(
    spark.table(GOLD_VIEW)
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
    )
    .orderBy("customer_id")
    .show(
        truncate=False,
    )
)


# =============================================================================
# DQX-VALID DELTA HISTORY
# =============================================================================

show_section("DQX-VALID DELTA HISTORY")

(
    spark.sql(
        f"DESCRIBE HISTORY {DQX_VALID_TABLE}"
    )
    .select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "operationMetrics",
    )
    .orderBy(col("version").asc())
    .show(
        truncate=False,
    )
)


# =============================================================================
# SILVER DELTA HISTORY
# =============================================================================

show_section("SILVER DELTA HISTORY")

(
    spark.sql(
        f"DESCRIBE HISTORY {SILVER_TABLE}"
    )
    .select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "operationMetrics",
    )
    .orderBy(col("version").asc())
    .show(
        truncate=False,
    )
)


# =============================================================================
# SUMMARY
# =============================================================================

show_section("POC 2 EVIDENCE COMPLETE")

if delete_commit is None:
    print(
        "Current state was displayed, but no DQX-valid DELETE commit "
        "was found."
    )
else:
    print(
        f"Displayed CDF evidence for DELETE commit "
        f"{int(delete_commit['version'])}."
    )

print(
    "The Silver table shows active and closed SCD2 versions, while Gold "
    "shows only current active customers."
)
