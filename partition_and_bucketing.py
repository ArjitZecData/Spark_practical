from pyspark.sql import SparkSession

# Spark session create karo
spark = SparkSession.builder \
    .appName("CSV read and write") \
    .getOrCreate()

# CSV file ko read karo
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("operation_files/write_data_into_disk.csv")

df.show()

# write into new folder  for partition

# df.write.format("csv") \
#     .partitionBy("address") \
#     .option("header","true") \
#     .mode("overwrite") \
#     .save("operation_files/data_write_into_disk/partition_by_address")


# spark.stop()


# write into new folder for bucketing feature

df.write.format("csv") \
    .bucketBy(3,"id") \
    .option("header","true") \
    .mode("overwrite") \
    .saveAsTable("bucketing_by_id")

spark.stop()
