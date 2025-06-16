from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

# create a data in list of tuple 
my_data = [("Arjit", 25), ("Manish", 30), ("Nikita", 27)]


# create schema of table 
columns = ["Name", "Age"]

df = spark.createDataFrame(data = my_data, schema=columns)
df.show()
