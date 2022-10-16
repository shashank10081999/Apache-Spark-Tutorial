import os
import sys
from curses.ascii import SP

from pyspark import SparkConf, SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


conf = SparkConf().setMaster("local").setAppName("customers_total_cost")
sc = SparkContext(conf=conf)
rdd = sc.textFile("Book")
friends_data = rdd.flatMap(lambda x:x.split())

friends_data_count = friends_data.countByValue()


for word,count in friends_data_count.items():
    print(word , count)
