from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read JSON Example") \
    .getOrCreate()

# Line-delimited JSON read
# json_df = spark.read.format("json") \
#     .option("inferSchema", True) \
#     .load("file.json")

# json_df.show()

# B. For multiline JSON:

json_df = spark.read.format("json") \
    .option("inferSchema", True) \
    .option("multiline", "true") \
    .load("main.json")

json_df.show()
json_df.printSchema()
