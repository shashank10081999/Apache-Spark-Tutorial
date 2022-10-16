import os
import sys
from curses.ascii import SP

from pyspark import SparkConf, SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def split_line(line):
    fields = line.split(",")
    age = int(fields[2])
    numfriends = int(fields[3])
    return (age,numfriends)


conf = SparkConf().setMaster("local").setAppName("customers_total_cost")
sc = SparkContext(conf=conf)
rdd = sc.textFile("fakefriends.csv")
friends_data = rdd.map(split_line)

totalByAge = friends_data.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y : (x[0]+y[0] , x[1]+y[1]))

averageByAge = totalByAge.mapValues(lambda x: x[0]/x[1])

results = averageByAge.collect()

for result in results:
    print(result)
