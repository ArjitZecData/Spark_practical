from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType ,DateType

spark = SparkSession.builder \
    .appName("Read Different File Formats") \
    .getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("_corrupted_data", StringType(), True)   #when you want to see data 
])


# How to deal with corrupted  records

# 1.PERMISSIVE mode 

csv_df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("mode","permissive") \
    .schema(schema) \
    .load("corrupt_customers.csv") 
csv_df.show()   


# 2.FAILFAST mode 


# csv_df = spark.read.format("csv") \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .option("mode","failfast") \
#     .schema(schema) \
#     .load("corrupt_customers.csv") 
# csv_df.show()   



# 3.DROPMALFORMED mode

# csv_df = spark.read.format("csv") \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .option("mode", "dropmalformed") \
#     .schema(schema) \
#     .load("corrupt_customers.csv")

# csv_df.show()
