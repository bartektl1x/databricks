"""
02_pipeline.py

Lakeflow pipeline for POC 1.

Purpose:
    Demonstrate propagation of a Delta CDF DELETE event into an AUTO CDC
    SCD Type 2 target.

Expected behavior:
    Deleting a source row causes AUTO CDC to close the active SCD2 version
    by setting __END_AT.

Important:
    The historical target record remains stored. This is logical SCD2
    deletion, not physical erasure.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.source_customers"
CDF_VIEW = "source_customers_cdf"
TARGET_TABLE = "satellite_customers_scd2"
FLOW_NAME = "apply_source_customers_cdf"


# =============================================================================
# SOURCE CDF VIEW
# =============================================================================

@dp.temporary_view(
    name=CDF_VIEW,
    comment=(
        "Streaming Delta Change Data Feed for source_customers. "
        "Update preimages are excluded."
    ),
)
def source_customers_cdf():
    """
    Read source_customers through Delta Change Data Feed.

    The first pipeline execution processes the current source snapshot.
    Subsequent executions process newly committed changes, including deletes.
    """

    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(SOURCE_TABLE)
        .filter(col("_change_type") != "update_preimage")
    )


# =============================================================================
# SCD TYPE 2 TARGET
# =============================================================================

dp.create_streaming_table(
    name=TARGET_TABLE,
    comment=(
        "POC SCD Type 2 customer satellite. "
        "Source deletes close active versions but preserve history."
    ),
)


# =============================================================================
# AUTO CDC FLOW
# =============================================================================

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
