import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

schema = StructType([ \
                     StructField("CustomerID", IntegerType(), True), \
                     StructField("CustomerNumber", IntegerType(), True), \
                     StructField("Summa", FloatType(), True)])

df = spark.read.schema(schema).csv("C:/Users/mpuga/PycharmProjects/spark_practice/dev/sparkcourse_udemy/resources/customer-orders.csv")
df.printSchema()

new_dataset = df.select("CustomerID", "Summa")
results = new_dataset.groupBy("CustomerID").agg(func.round(func.sum("Summa"), 0)\
                          .alias("Total_money")).sort(func.col("Total_money").desc())
results.show(results.count())

spark.stop()
