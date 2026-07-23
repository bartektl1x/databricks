"""Auto Loader Bronze plus DQX valid/quarantine. Auto-TTL is deferred."""
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, regexp_extract, to_timestamp
from pyspark.sql.types import StringType, StructField, StructType
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.sdk import WorkspaceClient
CATALOG = "main"
SCHEMA = "demo"
VOLUME = "poc2_source_files"
SOURCE_DIRECTORY = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/customers"
BRONZE_TABLE = "poc2_bronze_customers"
DQX_ANNOTATED_VIEW = "poc2_dqx_customers_annotated"
DQX_VALID_TABLE = "poc2_dqx_customers_valid"
DQX_QUARANTINE_TABLE = "poc2_dqx_customers_quarantine"
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
# FUTURE AUTO-TTL PLACEHOLDERS (enable only after predictive optimization approval):
# Bronze: auto_ttl={"timestamp_column": "_ingested_at", "expire_in_days": 30}
# Valid: auto_ttl={"timestamp_column": "_ingested_at", "expire_in_days": 20}
# Quarantine: auto_ttl={"timestamp_column": "_ingested_at", "expire_in_days": 10}
CSV_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("updated_at", StringType(), True),
])
DQ_ENGINE = DQEngine(WorkspaceClient())
DQ_CHECKS = [
    DQRowRule(name="customer_id_is_not_null", criticality="error", check_func=check_funcs.is_not_null, column="customer_id"),
    DQRowRule(name="email_has_valid_format", criticality="error", check_func=check_funcs.regex_match, column="email", check_func_kwargs={"regex": EMAIL_REGEX}),
    DQRowRule(name="updated_at_is_not_null", criticality="error", check_func=check_funcs.is_not_null, column="updated_at"),
]
@dp.table(name=BRONZE_TABLE, comment="Keyless Auto Loader Bronze; preserves _ingested_at for future TTL.")
def poc2_bronze_customers():
    source_df=(spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("header","true").schema(CSV_SCHEMA).load(SOURCE_DIRECTORY))
    return (source_df.withColumn("_updated_at_raw", col("updated_at"))
        .withColumn("updated_at", to_timestamp(col("_updated_at_raw")))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source_file_name", regexp_extract(col("_metadata.file_path"), r"([^/]+)$", 1))
        .select("customer_id","name","email","city","updated_at","_updated_at_raw","_ingested_at","_source_file","_source_file_name"))
@dp.temporary_view(name=DQX_ANNOTATED_VIEW, comment="DQX evaluation over Bronze.")
def poc2_dqx_customers_annotated():
    # Keep skipChangeCommits for future Bronze Auto-TTL DELETE commits.
    bronze_df=(spark.readStream.option("skipChangeCommits","true").table(BRONZE_TABLE)
        .withColumn("customer_id_passed", col("customer_id").isNotNull())
        .withColumn("email_passed", col("email").rlike(EMAIL_REGEX))
        .withColumn("updated_at_passed", col("updated_at").isNotNull()))
    return DQ_ENGINE.apply_checks(bronze_df, DQ_CHECKS)
@dp.table(name=DQX_VALID_TABLE, comment="DQX-valid rows with CDF; _ingested_at retained for future TTL.", table_properties={"delta.enableChangeDataFeed":"true"})
def poc2_dqx_customers_valid():
    return DQ_ENGINE.get_valid(spark.readStream.table(DQX_ANNOTATED_VIEW))
@dp.table(name=DQX_QUARANTINE_TABLE, comment="DQX-invalid rows; _ingested_at retained for future TTL.")
def poc2_dqx_customers_quarantine():
    return DQ_ENGINE.get_invalid(spark.readStream.table(DQX_ANNOTATED_VIEW))
