"""
POC 2.1 Pipeline 2.

Published DQX-valid CDF
    -> deterministic customer hash key
    -> insert-only AUTO CDC SCD Type 1 Hub
    -> AUTO CDC SCD Type 2 Satellite
    -> durable SCD Type 1 deletion markers
    -> classified Satellite materialized view
    -> Gold current-state materialized view

The marker target records actual source delete events without changing the
validated SCD2 invariant that a deleted customer has no active Silver row.
The Hub records business-key identity and deliberately ignores source updates
and deletes.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    concat_ws,
    expr,
    lit,
    sha2,
    trim,
    upper,
)


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.poc21_dqx_customers_valid"

CDF_VIEW = "poc21_dqx_customers_valid_cdf"
HUB_SOURCE_VIEW = "poc21_customer_hub_source"
DELETE_MARKER_SOURCE_VIEW = "poc21_customer_delete_marker_source"

HUB_TABLE = "poc21_hub_customers"
SILVER_TABLE = "poc21_satellite_customers_scd2"
MARKER_TABLE = "poc21_customer_deletion_markers"
CLASSIFIED_VIEW = "poc21_satellite_customers_classified"
GOLD_VIEW = "poc21_gold_customers_current"

HUB_FLOW_NAME = "apply_poc21_customer_hub"
SILVER_FLOW_NAME = "apply_poc21_dqx_customer_cdf"
MARKER_FLOW_NAME = "apply_poc21_customer_delete_markers"


# =============================================================================
# HASH-KEY CONTRACT
# =============================================================================

def customer_hash_key():
    """
    Return the deterministic POC customer Hub hash key.

    The normalization contract is deliberately explicit and must be reused by
    every Hub, Satellite, marker, and Gold flow:

        SHA-256("CUSTOMER" || "||" || UPPER(TRIM(customer_id)))

    The DQX-valid table guarantees a non-null customer_id.
    """

    return sha2(
        concat_ws(
            "||",
            lit("CUSTOMER"),
            upper(trim(col("customer_id"))),
        ),
        256,
    )


# =============================================================================
# KEYED DQX-VALID CDF
# =============================================================================

@dp.temporary_view(
    name=CDF_VIEW,
    comment=(
        "Streaming, hash-keyed CDF for the published POC 2.1 DQX-valid "
        "staging table. Update preimages are excluded."
    ),
)
def poc21_dqx_customers_valid_cdf():
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(SOURCE_TABLE)
        .filter(col("_change_type") != "update_preimage")
        .withColumn("customer_hk", customer_hash_key())
    )


# =============================================================================
# DATA VAULT HUB
# =============================================================================

@dp.temporary_view(
    name=HUB_SOURCE_VIEW,
    comment=(
        "Insert-only customer business-key discoveries for the POC 2.1 Hub. "
        "Attribute updates and source deletes do not mutate the Hub."
    ),
)
def poc21_customer_hub_source():
    return (
        spark.readStream
        .table(CDF_VIEW)
        .filter(col("_change_type") == "insert")
        .select(
            "customer_hk",
            "customer_id",
            lit("POC21_DQX_VALID").alias("record_source"),
            col("_commit_timestamp").alias("hub_loaded_at"),
            col("_commit_version").alias("hub_sequence"),
        )
    )


dp.create_streaming_table(
    name=HUB_TABLE,
    comment=(
        "POC 2.1 customer Data Vault Hub. Source updates and deletes never "
        "remove the business-key identity."
    ),
)

dp.create_auto_cdc_flow(
    name=HUB_FLOW_NAME,
    target=HUB_TABLE,
    source=HUB_SOURCE_VIEW,
    keys=["customer_hk"],
    sequence_by=col("hub_sequence"),
    except_column_list=["hub_sequence"],
    stored_as_scd_type=1,
)


# =============================================================================
# DATA VAULT SATELLITE SCD TYPE 2
# =============================================================================

dp.create_streaming_table(
    name=SILVER_TABLE,
    comment=(
        "POC 2.1 customer Data Vault SCD2 Satellite keyed by customer_hk. "
        "Source deletes close active versions and retain business history."
    ),
)

dp.create_auto_cdc_flow(
    name=SILVER_FLOW_NAME,
    target=SILVER_TABLE,
    source=CDF_VIEW,
    keys=["customer_hk"],
    sequence_by=col("_commit_version"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=[
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    ],
    stored_as_scd_type=2,
    # Only business satellite payload changes create SCD2 versions.
    # Operational ingestion metadata and DQX diagnostics must not create
    # additional business-history lifecycles.
    track_history_column_list=[
        "name",
        "email",
        "city",
        "updated_at",
    ],
)


# =============================================================================
# NORMALIZED DELETE-MARKER SOURCE
# =============================================================================

@dp.temporary_view(
    name=DELETE_MARKER_SOURCE_VIEW,
    comment=(
        "Delete-only marker records normalized from the POC 2.1 "
        "DQX-valid CDF."
    ),
)
def poc21_customer_delete_marker_source():
    return (
        spark.readStream
        .table(CDF_VIEW)
        .filter(col("_change_type") == "delete")
        .select(
            col("customer_hk"),
            col("customer_id"),
            col("_commit_version").alias("delete_sequence"),
            col("_commit_timestamp").alias("deleted_at"),
            lit("SOURCE_DELETE").alias("deletion_reason"),
            lit(None).cast("string").alias("deletion_request_id"),
        )
    )


# =============================================================================
# DURABLE SCD TYPE 1 DELETE MARKERS
# =============================================================================

dp.create_streaming_table(
    name=MARKER_TABLE,
    comment=(
        "Durable POC 2.1 deletion provenance. One logical marker per "
        "customer_hk and DQX delete sequence."
    ),
)

dp.create_auto_cdc_flow(
    name=MARKER_FLOW_NAME,
    target=MARKER_TABLE,
    source=DELETE_MARKER_SOURCE_VIEW,
    keys=[
        "customer_hk",
        "delete_sequence",
    ],
    sequence_by=col("delete_sequence"),
    stored_as_scd_type=1,
)


# =============================================================================
# CLASSIFIED SILVER HISTORY
# =============================================================================

@dp.materialized_view(
    name=CLASSIFIED_VIEW,
    comment=(
        "POC 2.1 Silver history enriched with deterministic "
        "update-versus-delete closure classification."
    ),
)
def poc21_satellite_customers_classified():
    silver_df = spark.read.table(SILVER_TABLE).alias("silver")
    marker_df = spark.read.table(MARKER_TABLE).alias("marker")

    join_condition = (
        (
            col("silver.customer_hk")
            == col("marker.customer_hk")
        )
        & (
            col("silver.__END_AT")
            == col("marker.delete_sequence")
        )
    )

    return (
        silver_df
        .join(
            marker_df,
            join_condition,
            "left",
        )
        .select(
            "silver.*",
            col("marker.delete_sequence"),
            col("marker.deleted_at"),
            col("marker.deletion_reason"),
            col("marker.deletion_request_id"),
            col("marker.delete_sequence")
            .isNotNull()
            .alias("closed_by_delete"),
        )
    )


# =============================================================================
# GOLD CURRENT STATE
# =============================================================================

@dp.materialized_view(
    name=GOLD_VIEW,
    comment=(
        "POC 2.1 current active customer state derived from Silver SCD2."
    ),
)
def poc21_gold_customers_current():
    hub_df = spark.read.table(HUB_TABLE).alias("hub")
    active_satellite_df = (
        spark.read
        .table(SILVER_TABLE)
        .filter(col("__END_AT").isNull())
        .alias("satellite")
    )

    return (
        hub_df
        .join(
            active_satellite_df,
            col("hub.customer_hk") == col("satellite.customer_hk"),
            "inner",
        )
        .select(
            col("hub.customer_hk"),
            col("hub.customer_id"),
            col("hub.record_source"),
            col("hub.hub_loaded_at"),
            col("satellite.name"),
            col("satellite.email"),
            col("satellite.city"),
            col("satellite.updated_at"),
            col("satellite._ingested_at"),
            col("satellite._source_file"),
            col("satellite._source_file_name"),
            col("satellite.customer_id_passed"),
            col("satellite.email_passed"),
            col("satellite.updated_at_passed"),
        )
    )
