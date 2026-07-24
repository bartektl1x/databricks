"""
Reset all isolated POC 2.1 objects.

Run this outside Lakeflow pipelines while both POC 2.1 pipelines are stopped.
After this script, seed the CSV and run a FULL REFRESH of both pipelines.
"""

from pathlib import Path

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "main"
SCHEMA = "demo"
VOLUME = "poc21_source_files"

SOURCE_DIRECTORY = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/customers"

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


# =============================================================================
# CREATE NAMESPACE
# =============================================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")


# =============================================================================
# DROP PIPELINE OUTPUTS
# =============================================================================

for view_name in [
    GOLD_VIEW,
    CLASSIFIED_VIEW,
]:
    spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")

for table_name in [
    MARKER_TABLE,
    SILVER_TABLE,
    HUB_TABLE,
    DQX_QUARANTINE_TABLE,
    DQX_VALID_TABLE,
    BRONZE_TABLE,
]:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# =============================================================================
# REMOVE SOURCE FILES
# =============================================================================

source_path = Path(SOURCE_DIRECTORY)
source_path.mkdir(parents=True, exist_ok=True)

for child in source_path.iterdir():
    assert child.is_file(), (
        f"Unexpected subdirectory in POC source directory: {child}"
    )
    child.unlink()

assert list(source_path.iterdir()) == [], (
    f"Expected an empty source directory at {SOURCE_DIRECTORY}."
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2.1 RESET COMPLETED")
print("=" * 80)
print(f"Catalog:          {CATALOG}")
print(f"Schema:           {SCHEMA}")
print(f"Source directory: {SOURCE_DIRECTORY}")
print()
print("Auto-TTL remains deferred.")
print()
print("Next steps:")
print("1. Run 01_seed_csv.py.")
print("2. FULL REFRESH Pipeline 1.")
print("3. FULL REFRESH Pipeline 2.")
print("=" * 80)
