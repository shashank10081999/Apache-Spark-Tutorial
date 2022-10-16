import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("most-watched-movie").master("local[*]").getOrCreate()

def loadMovieNames():
    movieNames = {}
    f = open("ml-100k/u.item","r",encoding="ISO-8859-1")
    list_lines = f.readlines()
    for i in list_lines:
        fields = i.split("|")
        movieNames[int(fields[0])] = fields[1]
    return movieNames

nameDict = spark.sparkContext.broadcast(loadMovieNames())


movie_len_Schema = StructType(
    [
        StructField("user_id",IntegerType(),True),
        StructField("movie_id",IntegerType(),True),
        StructField("rating",IntegerType(),True),
        StructField("time_stamp",LongType(),True)
    ]
)

df = spark.read.option("sep","\t").schema(movie_len_Schema).csv("ml-100k/u.data")

top_movie_id_byRating = df.groupBy("movie_id").sum("rating")

top_movie_id_byCount = df.groupBy("movie_id").count()


def lookUpNames(movieID):
    return nameDict.value[movieID]

loopUpNameUDF = func.udf(lookUpNames)

movieWithName = top_movie_id_byCount.withColumn("Movie Title" , loopUpNameUDF(func.col("movie_id")))

sortedMoiewithName = movieWithName.orderBy(func.desc("count"))

movieWithName_rating = top_movie_id_byRating.withColumn("Movie Title" , loopUpNameUDF(func.col("movie_id")))

sortedMoiewithName_rating = movieWithName_rating.orderBy(func.desc("sum(rating)"))

sortedMoiewithName.show(10)
sortedMoiewithName_rating.show(10)
