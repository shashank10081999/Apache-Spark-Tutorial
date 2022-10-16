import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("most-watched-movie").master("local[*]").getOrCreate()


movie_len_Schema = StructType(
    [
        StructField("user_id",IntegerType(),True),
        StructField("movie_id",IntegerType(),True),
        StructField("rating",IntegerType(),True),
        StructField("time_stamp",LongType(),True)
    ]
)

df = spark.read.option("sep","\t").schema(movie_len_Schema).csv("ml-100k/u.data")

top_movie_id_byRating = df.groupBy("movie_id").sum("rating").orderBy(func.desc("sum(rating)"))


top_movie_id_byCount = df.groupBy("movie_id").count().orderBy(func.desc("count"))

top_movie_id_byCount.show(10)
top_movie_id_byRating.show(10)
