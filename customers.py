import os
import sys
from curses.ascii import SP

from pyspark import SparkConf, SparkContext

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def split_line(line):
    fields = line.split(",")
    return (int(fields[0]),float(fields[2]))


conf = SparkConf().setMaster("local").setAppName("customers_total_cost")
sc = SparkContext(conf=conf)
rdd = sc.textFile("customer-orders.csv")
customer_data = rdd.map(split_line)


total_cost = customer_data.reduceByKey(lambda x,y : x+y)

total_cost_sorted = total_cost.map(lambda x:(x[1],x[0])).sortByKey()

results = total_cost_sorted.collect()

for result in results:
    print(result)
