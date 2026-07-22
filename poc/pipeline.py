"""
================================================================================
DELETE PROPAGATION POC - Lakeflow Declarative Pipeline
================================================================================

Simple end-to-end demonstration of CDC delete propagation:
  Source Table (CDF enabled)  -->  SCD Type 2 Satellite Table

Flow:
  1. DELETE from source table  -->  CDF emits _change_type='delete'
  2. Pipeline reads CDF        -->  source_customers_cdf view
  3. AUTO CDC flow applies     -->  closes record in SCD2 (sets __END_AT)
  4. Record preserved in history with __END_AT timestamp

Requirements:
  - Databricks Runtime 15.2+ (17.3 recommended)
  - Unity Catalog enabled
  - CDF enabled on source table

ADJUST THESE PARAMS FOR YOUR WORKSPACE:
"""
CATALOG = "main"          # <-- Change to your catalog
SCHEMA = "demo"           # <-- Change to your schema


# ================================================================================
# STEP 0: IMPORTS
# ================================================================================
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, struct


# ================================================================================
# STEP 1: SOURCE CDF VIEW
# ================================================================================
# Reads the Change Data Feed from the source table.
# CDF adds: _change_type, _commit_version, _commit_timestamp
# ================================================================================

@dp.temporary_view()
def source_customers_cdf():
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(f"{CATALOG}.{SCHEMA}.source_customers")
        .filter("_change_type != 'update_preimage'")   # Drop preimages
    )


# ================================================================================
# STEP 2: TARGET STREAMING TABLE (SCD Type 2)
# ================================================================================
# Must declare target BEFORE create_auto_cdc_flow.
# SCD Type 2 requires __START_AT and __END_AT columns (added automatically).
# ================================================================================

dp.create_streaming_table(
    name="satellite_customers_scd2",
    comment="SCD Type 2 satellite. Delete propagation closes records with __END_AT.",
    cluster_by=["customer_id"],
)


# ================================================================================
# STEP 3: AUTO CDC FLOW - DELETE PROPAGATION
# ================================================================================
# Keys: business key for matching
# sequence_by: ordering column(s) for out-of-order events
# apply_as_deletes: condition that identifies delete events from CDF
# stored_as_scd_type="2": preserves history, closes old version with __END_AT
# except_column_list: exclude CDF metadata from target table
# ================================================================================

dp.create_auto_cdc_flow(
    target="satellite_customers_scd2",
    source="source_customers_cdf",
    keys=["customer_id"],
    sequence_by=struct("updated_at", "_commit_version"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type="2",
)


# ================================================================================
# STEP 4: CONVENIENCE VIEW - ACTIVE RECORDS ONLY
# ================================================================================

@dp.temporary_view()
def satellite_customers_current():
    return spark.read.table("satellite_customers_scd2").filter("__END_AT IS NULL")


# ================================================================================
# HOW TO TEST (run these in a Databricks notebook after pipeline runs):
# ================================================================================
#
#   -- 1. Check source table
#   SELECT * FROM main.demo.source_customers;
#
#   -- 2. Delete a customer
#   DELETE FROM main.demo.source_customers WHERE customer_id = 'C002';
#
#   -- 3. Trigger pipeline update, then check satellite
#   SELECT customer_id, name, __START_AT, __END_AT,
#          CASE WHEN __END_AT IS NULL THEN 'ACTIVE' ELSE 'CLOSED' END as status
#   FROM main.demo.satellite_customers_scd2
#   WHERE customer_id = 'C002';
#
#   -- Expected: C002 has __END_AT set (closed record), not physically deleted
#
# ================================================================================
