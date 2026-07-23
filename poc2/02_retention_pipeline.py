"""
Lakeflow retention pipeline for POC 2.

Flow:
    CSV Volume
      -> Auto Loader Bronze streaming table with Auto-TTL
      -> DQX annotated temporary streaming view
      -> valid streaming table with independent Auto-TTL and legacy CDF
      -> quarantine streaming table with independent Auto-TTL

This pipeline intentionally remains hard-coded and isolated from production
framework classes.

Pipeline publication target:
    Catalog: dev_mr_dhc_bronze
    Schema:  slpat_landing_staging

Pipeline environment dependency:
    databricks-labs-dqx
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    current_timestamp,
    regexp_extract,
    to_timestamp,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.sdk import WorkspaceClient


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "dev_mr_dhc_bronze"
SCHEMA = "slpat_landing_staging"
VOLUME = "poc2_source_files"

SOURCE_DIRECTORY = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/customers"

BRONZE_TABLE = "poc2_bronze_customers"
DQX_ANNOTATED_VIEW = "poc2_dqx_customers_annotated"
DQX_VALID_TABLE = "poc2_dqx_customers_valid"
DQX_QUARANTINE_TABLE = "poc2_dqx_customers_quarantine"

# Deliberately short POC values. Auto-TTL runs asynchronously, so deterministic
# tests verify policy configuration separately from physical execution timing.
BRONZE_TTL_DAYS = 30
DQX_VALID_TTL_DAYS = 20
DQX_QUARANTINE_TTL_DAYS = 10

EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


# =============================================================================
# INPUT SCHEMA
# =============================================================================

CSV_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)


# =============================================================================
# DQX ENGINE AND RULES
# =============================================================================

DQ_ENGINE = DQEngine(WorkspaceClient())

DQ_CHECKS = [
    DQRowRule(
        name="customer_id_is_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="customer_id",
    ),
    DQRowRule(
        name="email_has_valid_format",
        criticality="error",
        check_func=check_funcs.regex_match,
        column="email",
        check_func_kwargs={"regex": EMAIL_REGEX},
    ),
    DQRowRule(
        name="updated_at_is_not_null",
        criticality="error",
        check_func=check_funcs.is_not_null,
        column="updated_at",
    ),
]


# =============================================================================
# AUTO LOADER BRONZE
# =============================================================================

@dp.table(
    name=BRONZE_TABLE,
    comment=(
        "POC 2 keyless Bronze ingestion from CSV files using Auto Loader. "
        "Rows expire independently according to preserved _ingested_at."
    ),
    auto_ttl={
        "timestamp_column": "_ingested_at",
        "expire_in_days": BRONZE_TTL_DAYS,
    },
)
def poc2_bronze_customers():
    """
    Ingest CSV files incrementally.

    Bronze deliberately performs no business-key enforcement. It preserves the
    raw source fields and adds ingestion/file metadata.
    """

    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(CSV_SCHEMA)
        .load(SOURCE_DIRECTORY)
    )

    return (
        source_df
        .withColumn("_updated_at_raw", col("updated_at"))
        .withColumn("updated_at", to_timestamp(col("_updated_at_raw")))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn(
            "_source_file_name",
            regexp_extract(col("_metadata.file_path"), r"([^/]+)$", 1),
        )
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
    )


# =============================================================================
# DQX ANNOTATION
# =============================================================================

@dp.temporary_view(
    name=DQX_ANNOTATED_VIEW,
    comment="DQX results over Bronze with explicit row-level pass indicators.",
)
def poc2_dqx_customers_annotated():
    """
    Read Bronze with skipChangeCommits.

    Auto-TTL DELETE commits from Bronze must not break this stream and must not
    propagate into DQX. DQX owns its independent retention policy.
    """

    bronze_df = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table(BRONZE_TABLE)
        .withColumn("customer_id_passed", col("customer_id").isNotNull())
        .withColumn("email_passed", col("email").rlike(EMAIL_REGEX))
        .withColumn("updated_at_passed", col("updated_at").isNotNull())
    )

    return DQ_ENGINE.apply_checks(bronze_df, DQ_CHECKS)


# =============================================================================
# DQX VALID
# =============================================================================

@dp.table(
    name=DQX_VALID_TABLE,
    comment=(
        "Rows accepted by DQX. Uses an independent Auto-TTL policy and exposes "
        "legacy Delta CDF for downstream Data Vault AUTO CDC."
    ),
    auto_ttl={
        "timestamp_column": "_ingested_at",
        "expire_in_days": DQX_VALID_TTL_DAYS,
    },
    table_properties={
        "delta.enableChangeDataFeed": "true",
    },
)
def poc2_dqx_customers_valid():
    annotated_df = spark.readStream.table(DQX_ANNOTATED_VIEW)
    return DQ_ENGINE.get_valid(annotated_df)


# =============================================================================
# DQX QUARANTINE
# =============================================================================

@dp.table(
    name=DQX_QUARANTINE_TABLE,
    comment=(
        "Rows rejected by DQX with detailed DQX result columns. Uses an "
        "independent Auto-TTL policy."
    ),
    auto_ttl={
        "timestamp_column": "_ingested_at",
        "expire_in_days": DQX_QUARANTINE_TTL_DAYS,
    },
)
def poc2_dqx_customers_quarantine():
    annotated_df = spark.readStream.table(DQX_ANNOTATED_VIEW)
    return DQ_ENGINE.get_invalid(annotated_df)
