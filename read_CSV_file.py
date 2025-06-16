from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType ,DateType

spark = SparkSession.builder \
    .appName("Read Different File Formats") \
    .getOrCreate()



schema = StructType([
    StructField("Index", IntegerType(), True),
    StructField("Customer Id", StringType(), True),
    StructField("First Name", StringType(), True),
    StructField("Last Name", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Phone 1", StringType(), True),
    StructField("Phone 2", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Subscription Date", DateType(), True),
    StructField("Website", StringType(), True)
])



csv_df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .schema(schema) \
    .load("customers.csv") 
csv_df.show(5)    # if you don't give any no. then default result show 20 rows only
csv_df.printSchema()

