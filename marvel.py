import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("most-watched-movie").getOrCreate()


Schema = StructType(
    [
        StructField("id",IntegerType(),True),
        StructField("name",StringType(),True)
    ]
)

name_f = spark.read.option("sep"," ").schema(Schema).csv("Marvel+Names")

line = spark.read.text("Marvel+Graph")

connections = line.withColumn("id" , func.split(func.col("value")," ")[0])\
    .withColumn("connections",func.size(func.split(func.col("value")," "))-1)\
        .groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = name_f.filter(func.col("id") == mostPopular[0]).select("name").first()


print(mostPopularName[0])


minConnectionCount = connections.agg(func.min("connections")).first()[0]

minConnection = connections.filter(func.col("connections") == minConnectionCount)

minConnectionwithNames = minConnection.join(name_f , "id")

minConnectionwithNames.show()
