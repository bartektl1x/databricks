"""Assert deterministic state after both pipeline FULL REFRESH operations."""
from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark=SparkSession.builder.getOrCreate()
CATALOG="main"; SCHEMA="demo"
BRONZE=f"{CATALOG}.{SCHEMA}.poc2_bronze_customers"
VALID=f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"
QUARANTINE=f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_quarantine"
SATELLITE=f"{CATALOG}.{SCHEMA}.poc2_satellite_customers_scd2"
GOLD=f"{CATALOG}.{SCHEMA}.poc2_gold_customers_current"
def eq(a:Any,e:Any,d:str): assert a==e, f"{d}: expected {e!r}, found {a!r}."
b=spark.table(BRONZE); v=spark.table(VALID); q=spark.table(QUARANTINE); s=spark.table(SATELLITE); g=spark.table(GOLD)
eq(b.count(),5,"Bronze rows"); eq(v.count(),3,"Valid rows"); eq(q.count(),2,"Quarantine rows")
eq({r.customer_id for r in v.select("customer_id").collect()},{"C001","C002","C004"},"Valid IDs")
eq({r.name for r in q.select("name").collect()},{"Charlie Brown","Missing Identifier"},"Quarantine names")
eq(v.filter(~(col("customer_id_passed") & col("email_passed") & col("updated_at_passed"))).count(),0,"Invalid flags in valid rows")
charlie=q.filter(col("name")=="Charlie Brown").first(); missing=q.filter(col("name")=="Missing Identifier").first()
assert charlie and missing
eq((charlie.customer_id_passed,charlie.email_passed,charlie.updated_at_passed),(True,False,True),"Charlie flags")
eq((missing.customer_id_passed,missing.email_passed,missing.updated_at_passed),(False,True,True),"Missing-ID flags")
for name,df in [(BRONZE,b),(VALID,v),(QUARANTINE,q)]: eq(df.filter(col("_ingested_at").isNull()).count(),0,f"Null _ingested_at in {name}")
bronze_ts={r.name:r._ingested_at for r in b.select("name","_ingested_at").collect()}
for r in v.select("name","_ingested_at").collect()+q.select("name","_ingested_at").collect(): eq(r._ingested_at,bronze_ts[r.name],f"Preserved _ingested_at for {r.name}")
props={r.key:r.value for r in spark.sql(f"SHOW TBLPROPERTIES {VALID}").collect()}
eq(props.get("delta.enableChangeDataFeed","").lower(),"true","CDF property")
cdf=spark.sql(f"SELECT customer_id,_change_type FROM table_changes('{VALID}',0)")
eq(cdf.count(),3,"Initial CDF count"); eq({r._change_type for r in cdf.select("_change_type").distinct().collect()},{"insert"},"Initial CDF types")
eq(s.count(),3,"Silver history rows"); eq(s.filter(col("__END_AT").isNull()).count(),3,"Active Silver rows"); eq(s.filter(col("__END_AT").isNotNull()).count(),0,"Closed Silver rows")
eq(g.count(),3,"Gold rows"); eq({r.customer_id for r in g.select("customer_id").collect()},{"C001","C002","C004"},"Gold IDs")
print("POC 2 initial-state assertions passed. Next: run 05_delete_dqx_valid_row.py.")
