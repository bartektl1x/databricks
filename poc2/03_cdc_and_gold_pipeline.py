"""
Lakeflow CDC and Gold pipeline for POC 2.

Flow:
    Published DQX valid legacy CDF
      -> temporary streaming CDF view
      -> AUTO CDC SCD Type 2 satellite
      -> Gold materialized view of active records

The AUTO CDC implementation deliberately preserves the experimentally validated
POC 1 pattern.

Pipeline publication target:
    Catalog: dev_mr_dhc_bronze
    Schema:  slpat_landing_staging
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "dev_mr_dhc_bronze"
SCHEMA = "slpat_landing_staging"

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
CDF_VIEW = "poc2_dqx_customers_valid_cdf"
TARGET_TABLE = "poc2_satellite_customers_scd2"
GOLD_VIEW = "poc2_gold_customers_current"
FLOW_NAME = "apply_poc2_dqx_customer_cdf"


# =============================================================================
# PUBLISHED DQX CDF
# =============================================================================

@dp.temporary_view(
    name=CDF_VIEW,
    comment=(
        "Streaming legacy Delta Change Data Feed for the POC 2 DQX-valid "
        "table. Update preimages are excluded."
    ),
)
def poc2_dqx_customers_valid_cdf():
    """
    The initial execution processes the current DQX-valid snapshot.
    Subsequent executions process new CDF events, including deletes.
    """

    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(SOURCE_TABLE)
        .filter(col("_change_type") != "update_preimage")
    )


# =============================================================================
# SCD TYPE 2 SATELLITE
# =============================================================================

dp.create_streaming_table(
    name=TARGET_TABLE,
    comment=(
        "POC 2 customer SCD Type 2 satellite. DQX-valid deletes close active "
        "versions while preserving historical rows."
    ),
)


dp.create_auto_cdc_flow(
    name=FLOW_NAME,
    target=TARGET_TABLE,
    source=CDF_VIEW,
    keys=["customer_id"],
    sequence_by=col("_commit_version"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=[
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    ],
    stored_as_scd_type=2,
)


# =============================================================================
# GOLD CURRENT-STATE MATERIALIZED VIEW
# =============================================================================

@dp.materialized_view(
    name=GOLD_VIEW,
    comment="Current active customer state projected from the SCD2 satellite.",
)
def poc2_gold_customers_current():
    return (
        spark.read.table(TARGET_TABLE)
        .filter(col("__END_AT").isNull())
        .select(
            "customer_id",
            "name",
            "email",
            "city",
            "updated_at",
            "_ingested_at",
            "_source_file",
            "_source_file_name",
            "customer_id_passed",
            "email_passed",
            "updated_at_passed",
        )
    )
