from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as func
spark=SparkSession.builder.appName("MostObscureSuperHeroes").getOrCreate()
schema=StructType([\
    StructField("Id",IntegerType(),True),\
    StructField("Name",StringType(),True)])

MarvelNames=spark.read.option("sep"," ").schema(schema).csv("Marvel+Names")

lines=spark.read.text("Marvel+Graph")
lines=lines.withColumn("Id",func.split(func.col("value")," ")[0])\
.withColumn("Connections",func.size(func.split(func.col("value")," "))-1)\
.groupBy("Id").agg(func.sum("Connections").alias("Connections")).sort(func.col("Connections"))
min=lines.agg(func.min("Connections")).first()

obscure=lines.filter(func.col("Connections")== min[0]).join(MarvelNames,"Id").select("Name").show()
              
