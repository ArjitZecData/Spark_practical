# how to read parquets file 
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Parquet Example") \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet("operation_files/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")

# Display the content
df.show()

# Display schema
df.printSchema()
