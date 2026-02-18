from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col
import time

def run_config(label, executor_memory, executor_cores):
   print(f"\n=== Config Test: {label} ===")

   spark = (
       SparkSession.builder
       .appName(f"ExecutorConfigTest_{label}")
       .config("spark.executor.memory", executor_memory)
       .config("spark.executor.cores", executor_cores)
       .getOrCreate()
   )

   orders = (
       spark.read.option("header", "true").option("inferSchema", "true")
       .csv("data/orders.csv")
   )
   order_items = (
       spark.read.option("header", "true").option("inferSchema", "true")
       .csv("data/order_items.csv")
   )

   df = orders.join(order_items, "order_id")
   df = df.withColumn("line_amount", col("quantity") * col("price"))

   start = time.time()
   df.groupBy("order_date").agg(spark_sum("line_amount")).count()
   duration = time.time() - start

   print(f"Executor Memory : {executor_memory}")
   print(f"Executor Cores  : {executor_cores}")
   print(f"Duration        : {duration:.2f} sec")

   spark.stop()

# Config A: banyak core, memory kecil
run_config("A (large cores, small memory)", "1g", "4")

# Config B: memory lebih besar, core sedikit
run_config("B (large memory, fewer cores)", "3g", "1")

# Config C: balance
run_config("C (balanced)", "2g", "2")

print("\nTest complete.")
