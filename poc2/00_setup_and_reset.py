"""
POC 2 setup and reset.

Run outside Lakeflow pipelines.

Creates:
    dev_mr_dhc_bronze.slpat_landing_staging
    /Volumes/dev_mr_dhc_bronze/slpat_landing_staging/poc2_source_files

Drops only POC 2 objects. It does not modify the validated POC 1 objects.
After running this script, run both pipelines with FULL REFRESH.
"""

from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG = "dev_mr_dhc_bronze"
SCHEMA = "slpat_landing_staging"
VOLUME = "poc2_source_files"

SOURCE_DIRECTORY = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/customers"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_bronze_customers"
DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
DQX_QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_quarantine"
SATELLITE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_satellite_customers_scd2"
GOLD_VIEW = f"{CATALOG}.{SCHEMA}.poc2_gold_customers_current"


# =============================================================================
# CREATE NAMESPACE AND VOLUME
# =============================================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# Auto-TTL requires predictive optimization.
spark.sql(f"ALTER SCHEMA {CATALOG}.{SCHEMA} ENABLE PREDICTIVE OPTIMIZATION")


# =============================================================================
# DROP ONLY POC 2 DATASETS
# =============================================================================

spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {GOLD_VIEW}")

for table_name in [
    SATELLITE_TABLE,
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
    if child.is_file():
        child.unlink()

assert list(source_path.iterdir()) == [], (
    f"Expected an empty source directory, but found files in {SOURCE_DIRECTORY}."
)


# =============================================================================
# OUTPUT
# =============================================================================

print("=" * 80)
print("POC 2 RESET COMPLETED")
print("=" * 80)
print(f"Catalog:          {CATALOG}")
print(f"Schema:           {SCHEMA}")
print(f"Source directory: {SOURCE_DIRECTORY}")
print()
print("Next steps:")
print("1. Run 01_seed_csv.py.")
print("2. Run FULL REFRESH on the retention pipeline.")
print("3. Run FULL REFRESH on the CDC pipeline.")
print("4. Run 04_assert_initial_state.py.")
print("=" * 80)
