"""Reset isolated POC 2 objects. Run outside Lakeflow pipelines."""
from pathlib import Path
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
CATALOG = "main"
SCHEMA = "demo"
VOLUME = "poc2_source_files"
SOURCE_DIRECTORY = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/customers"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_bronze_customers"
DQX_VALID_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
DQX_QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_quarantine"
SATELLITE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_satellite_customers_scd2"
GOLD_VIEW = f"{CATALOG}.{SCHEMA}.poc2_gold_customers_current"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {GOLD_VIEW}")
for table_name in [SATELLITE_TABLE, DQX_QUARANTINE_TABLE, DQX_VALID_TABLE, BRONZE_TABLE]:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
source_path = Path(SOURCE_DIRECTORY)
source_path.mkdir(parents=True, exist_ok=True)
for child in source_path.iterdir():
    if child.is_file(): child.unlink()
    else: raise AssertionError(f"Unexpected subdirectory: {child}")
assert list(source_path.iterdir()) == []
print("POC 2 reset completed; Auto-TTL remains deferred.")
print("Next: run 01_seed_csv.py, then FULL REFRESH both pipelines.")
