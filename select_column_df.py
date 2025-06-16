from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType ,DateType
from pyspark.sql.functions import col, expr

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
    .load("operation_files/customers.csv") 


print("✅ 2. String Notation ")
csv_df.select("Index","company").show()


print("✅ 3. Using col() function")
csv_df.select(col("Index"), col("Email")).show(5)

print("✅ 4. Using expr() function    [when you want to perform calculation on specific column]")
csv_df.select(expr("Index + 6")).show()
