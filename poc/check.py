"""
test_deletes.py - Run this after the pipeline has processed at least once.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.getOrCreate()

CATALOG = "main"
SCHEMA = "demo"

print("=== ACTIVE RECORDS ===")
spark.read.table(f"{CATALOG}.{SCHEMA}.satellite_customers_scd2") \
    .filter("__END_AT IS NULL") \
    .show(truncate=False)

print("=== DELETING C002 FROM SOURCE ===")
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA}.source_customers WHERE customer_id = 'C002'")
print("Done. Now trigger a pipeline update and re-run this script.")

print("=== FULL HISTORY (run after pipeline update) ===")
df = spark.read.table(f"{CATALOG}.{SCHEMA}.satellite_customers_scd2")
df.withColumn("status", when(df["__END_AT"].isNull(), "ACTIVE").otherwise("CLOSED")) \
  .select("customer_id", "name", "city", "__START_AT", "__END_AT", "status") \
  .orderBy("customer_id", "__START_AT") \
  .show(truncate=False)
