"""Create deterministic DQX-valid DELETE and verify its CDF event."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark=SparkSession.builder.getOrCreate()
CATALOG="main"; SCHEMA="demo"; TABLE=f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"; TEST_ID="C002"
before=spark.table(TABLE).filter(col("customer_id")==TEST_ID).count()
assert before==1, f"Expected one {TEST_ID} row, found {before}. Reset POC 2."
spark.sql(f"DELETE FROM {TABLE} WHERE customer_id = '{TEST_ID}'")
assert spark.table(TABLE).filter(col("customer_id")==TEST_ID).count()==0
deletes=spark.sql(f"""SELECT customer_id,name,email,city,updated_at,_ingested_at,_change_type,_commit_version,_commit_timestamp FROM table_changes('{TABLE}',0) WHERE customer_id='{TEST_ID}' AND _change_type='delete'""")
assert deletes.count()==1, "Expected exactly one CDF delete event."
print("DQX-valid DELETE created. Run a NORMAL INCREMENTAL CDC/Gold pipeline update, then 06_assert_delete_propagation.py.")
deletes.show(truncate=False)
