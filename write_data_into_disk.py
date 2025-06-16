from pyspark.sql import SparkSession

# Spark session create karo
spark = SparkSession.builder \
    .appName("CSV read and write") \
    .getOrCreate()

# CSV file ko read karo
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("write_data_into_disk.csv")

df.show()

# write into new folder
df.write.format("csv") \
    .option("header","true") \
    .mode("overwrite") \
    .save("operation_files/data_write_into_disk/")


spark.stop()
