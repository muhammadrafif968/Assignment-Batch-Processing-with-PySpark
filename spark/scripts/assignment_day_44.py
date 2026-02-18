from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, to_date, col, sum as spark_sum

spark = SparkSession.builder.appName("batch-orders-pipeline").getOrCreate()

orders = spark.read.csv("/opt/airflow/data/orders.csv", header=True, inferSchema=True)
order_items = spark.read.csv("/opt/airflow/data/order_items.csv", header=True, inferSchema=True)
#Soal 2
#join orders dengan order_items
fact_df = orders.join(
    order_items,
    on="order_id",
    how="inner"
)
#Soal 3
fact_clean = (
    fact_df
    #filter quantity kalau ada yang null
    .filter(col("quantity").isNotNull())
    #ganti tipe data kolumn price dengan menjadi double dengan menggunakan cast
    .withColumn("price", col("price").cast("double"))
    #ganti tipe data colum quantity menjadi int dengan menggunakan cast
    .withColumn("quantity", col("quantity").cast("int"))
    #standarisasi format tanggal pada kolom order_date
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
)
#buat kolom gmv
fact_enriched = fact_clean.withColumn(
    "gmv",
    col("price") * col("quantity")
)

#Soal 3
#output hasil akhir dalam bentuk parquet pake overwite
#diletakin di file tmp karena ke bind windows dan dah pake chmod buat ngasi permsission tapi tetep ngak mau (aku pake wsl ubuntu)
output_path = "/tmp/fact_orders.parquet"

fact_enriched.write.mode("overwrite").parquet(output_path)

#check apakah file parquet nya bisa dibaca dengan spark
df_check = spark.read.parquet(output_path)
df_check.show(5, truncate=False)

#jalanin "docker cp spark-master:/tmp/fact_orders.parquet ./spark/data/" buat copy manual hasil ke spark data ( btw ini berhasil dibaca dari tmp/fact order parquet)
# 26/02/16 02:37:56 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
# {
#   "type" : "struct",
#   "fields" : [ {
#     "name" : "order_id",
#     "type" : "integer",
#     "nullable" : true,
#     "metadata" : { }
#   }, {
#     "name" : "user_id",
#     "type" : "integer",
#     "nullable" : true,
#     "metadata" : { }
#   }, {
#     "name" : "order_date",
#     "type" : "date",
#     "nullable" : true,
#     "metadata" : { }
#   }, {
#     "name" : "total_amount",
#     "type" : "integer",
#     "nullable" : true,
#     "metadata" : { }
#   }, {
#     "name" : "product_id",
#     "type" : "integer",
#     "nullable" : true,
#     "metadata" : { }
#   }, {
#     "name" : "quantity",
#     "type" : "integer",
#     "nullable" : true,
#     "metadata" : { }
#   }, {
#     "name" : "price",
#     "type" : "double",
#     "nullable" : true,
#     "metadata" : { }
#   }, {
#     "name" : "gmv",
#     "type" : "double",
#     "nullable" : true,
#     "metadata" : { }
#   } ]
# }
# and corresponding Parquet message type:
# message spark_schema {
#   optional int32 order_id;
#   optional int32 user_id;
#   optional int32 order_date (DATE);
#   optional int32 total_amount;
#   optional int32 product_id;
#   optional int32 quantity;
#   optional double price;
#   optional double gmv;
# }

# +--------+-------+----------+------------+----------+--------+---------+---------+
# |order_id|user_id|order_date|total_amount|product_id|quantity|price    |gmv      |
# +--------+-------+----------+------------+----------+--------+---------+---------+
# |5001    |375    |2023-04-23|2115104     |483       |1       |911307.0 |911307.0 |
# |5002    |201    |2023-03-12|2018053     |441       |3       |1028008.0|3084024.0|
# |5003    |144    |2023-01-30|1143270     |325       |4       |1429849.0|5719396.0|
# |5004    |543    |2023-05-15|2531019     |105       |3       |63923.0  |191769.0 |
# |5005    |405    |2023-03-08|875126      |406       |3       |1682060.0|5046180.0|
# +--------+-------+----------+------------+----------+--------+---------+---------+
# only showing top 5 rows