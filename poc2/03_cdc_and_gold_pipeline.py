"""DQX CDF -> AUTO CDC SCD2 Silver -> Gold materialized view."""
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr
CATALOG = "main"
SCHEMA = "demo"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
CDF_VIEW = "poc2_dqx_customers_valid_cdf"
TARGET_TABLE = "poc2_satellite_customers_scd2"
GOLD_VIEW = "poc2_gold_customers_current"
FLOW_NAME = "apply_poc2_dqx_customer_cdf"
@dp.temporary_view(name=CDF_VIEW, comment="Streaming CDF for published DQX-valid table; update preimages excluded.")
def poc2_dqx_customers_valid_cdf():
    return (spark.readStream.option("readChangeFeed","true").table(SOURCE_TABLE)
        .filter(col("_change_type") != "update_preimage"))
dp.create_streaming_table(name=TARGET_TABLE, comment="Silver SCD2 satellite; deletes close active versions and retain history.")
dp.create_auto_cdc_flow(
    name=FLOW_NAME,
    target=TARGET_TABLE,
    source=CDF_VIEW,
    keys=["customer_id"],
    sequence_by=col("_commit_version"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type","_commit_version","_commit_timestamp"],
    stored_as_scd_type=2,
)
@dp.materialized_view(name=GOLD_VIEW, comment="Current active customer state from Silver SCD2.")
def poc2_gold_customers_current():
    return (spark.read.table(TARGET_TABLE).filter(col("__END_AT").isNull())
        .select("customer_id","name","email","city","updated_at","_ingested_at","_source_file","_source_file_name","customer_id_passed","email_passed","updated_at_passed"))
