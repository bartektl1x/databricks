"""Display non-destructive POC 2 evidence."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
spark=SparkSession.builder.getOrCreate()
CATALOG="main"; SCHEMA="demo"; TEST_ID="C002"
B=f"{CATALOG}.{SCHEMA}.poc2_bronze_customers"; V=f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_valid"; Q=f"{CATALOG}.{SCHEMA}.poc2_dqx_customers_quarantine"; S=f"{CATALOG}.{SCHEMA}.poc2_satellite_customers_scd2"; G=f"{CATALOG}.{SCHEMA}.poc2_gold_customers_current"
def title(x): print("\n"+"="*100+"\n"+x+"\n"+"="*100)
title("BRONZE"); spark.table(B).orderBy("name").show(truncate=False)
title("DQX VALID"); spark.table(V).orderBy("customer_id").show(truncate=False)
title("DQX QUARANTINE"); spark.table(Q).orderBy("name").show(truncate=False)
title(f"DQX CDF FOR {TEST_ID}"); spark.sql(f"SELECT * FROM table_changes('{V}',0) WHERE customer_id='{TEST_ID}' ORDER BY _commit_version,_change_type").show(truncate=False)
title("SILVER SCD2"); spark.table(S).withColumn("record_status",when(col("__END_AT").isNull(),"ACTIVE").otherwise("CLOSED")).orderBy("customer_id","__START_AT").show(truncate=False)
title("GOLD"); spark.table(G).orderBy("customer_id").show(truncate=False)
title("DQX VALID HISTORY"); spark.sql(f"DESCRIBE HISTORY {V}").select("version","timestamp","operation","operationParameters","operationMetrics").orderBy("version").show(truncate=False)
title("SILVER HISTORY"); spark.sql(f"DESCRIBE HISTORY {S}").select("version","timestamp","operation","operationParameters","operationMetrics").orderBy("version").show(truncate=False)
