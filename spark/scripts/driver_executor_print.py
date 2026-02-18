from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, concat_ws, sum as spark_sum
import time

spark = SparkSession.builder \
  .appName("testing-driver") \
  .getOrCreate()

sc = spark.sparkContext

conf = sc.getConf()

def show_conf(key, label):
   value = conf.get(key, None)
   if value is None:
       value = "<not set (using default)>"
   print(f"{label}: {value}")

print("\n=== Spark Configuration (High Level) ===")
print("App Name:", sc.appName)
print("Master:", sc.master)
print("Default Parallelism:", sc.defaultParallelism)

# Executor count (approximate)
jsc = sc._jsc
num_executors = jsc.sc().getExecutorMemoryStatus().size()
print("Approx Executor Count:", num_executors)

print("\n=== Spark Driver Configuration ===")
show_conf("spark.driver.host", "Driver Host")
show_conf("spark.driver.port", "Driver Port")
show_conf("spark.driver.memory", "Driver Memory")
show_conf("spark.driver.cores", "Driver Cores")

print("\n=== Spark Executor Configuration ===")
show_conf("spark.executor.instances", "Executor Instances")
show_conf("spark.executor.memory", "Executor Memory")
show_conf("spark.executor.cores", "Executor Cores")
show_conf("spark.executor.memoryOverhead", "Executor Memory Overhead")
