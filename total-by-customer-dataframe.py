from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql import functions as func
spark=SparkSession.builder.appName('totalByCustomer').getOrCreate()

schema= StructType([StructField("UserId",IntegerType(),True),StructField("ProductId",IntegerType(),True),StructField("Price",FloatType(),True)])

customers=spark.read.schema(schema).csv("customer-orders.csv")
customers.printSchema()
customers_reduced=customers.select(customers.UserId,customers.Price)
customers_total=customers_reduced.groupBy("UserId").agg(func.round(func.sum("Price"),2).alias("total_price")).sort("total_price").show()
spark.stop()
