"""Assert DQX DELETE -> CDF -> AUTO CDC SCD2 closure -> Gold removal."""
from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark=SparkSession.builder.getOrCreate()
CATALOG="main"; SCHEMA="demo"; TEST_ID="C002"
VALID=f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"; SAT=f"{CATALOG}.{SCHEMA}.poc2_satellite_customers_scd2"; GOLD=f"{CATALOG}.{SCHEMA}.poc2_gold_customers_current"
def eq(a:Any,e:Any,d:str): assert a==e, f"{d}: expected {e!r}, found {a!r}."
valid=spark.table(VALID).filter(col("customer_id")==TEST_ID); hist=spark.table(SAT).filter(col("customer_id")==TEST_ID); active=hist.filter(col("__END_AT").isNull()); closed=hist.filter(col("__END_AT").isNotNull()); gold=spark.table(GOLD).filter(col("customer_id")==TEST_ID)
deletes=spark.sql(f"SELECT * FROM table_changes('{VALID}',0) WHERE customer_id='{TEST_ID}' AND _change_type='delete'")
eq(valid.count(),0,"Valid rows"); eq(deletes.count(),1,"CDF deletes"); eq(hist.count(),1,"Silver history"); eq(active.count(),0,"Silver active"); eq(closed.count(),1,"Silver closed"); eq(gold.count(),0,"Gold rows")
r=closed.select("customer_id","name","email","city","_ingested_at","customer_id_passed","email_passed","updated_at_passed","__START_AT","__END_AT").first(); assert r
eq((r.customer_id,r.name,r.email,r.city),("C002","Bob Smith","bob@example.com","Los Angeles"),"Retained attributes")
assert r._ingested_at is not None and r.customer_id_passed and r.email_passed and r.updated_at_passed
assert r.__START_AT is not None and r.__END_AT is not None and r.__END_AT > r.__START_AT
expected={"C001","C004"}
eq({x.customer_id for x in spark.table(SAT).filter(col("__END_AT").isNull()).select("customer_id").collect()},expected,"Active Silver IDs")
eq({x.customer_id for x in spark.table(GOLD).select("customer_id").collect()},expected,"Gold IDs")
print("POC 2 delete-propagation assertions passed: SCD2 closed history; Gold removed C002.")
hist.orderBy("__START_AT").show(truncate=False)
