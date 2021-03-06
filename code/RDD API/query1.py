from pyspark.sql import SparkSession

from io import StringIO
import csv

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

sc = spark.sparkContext

#Given function to parse the titles correct, because some of them has ","
def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]


res = \
        sc.textFile("hdfs://master:9000/movies/movies.csv"). \
        map(lambda x : (split_complex(x)[3][0:4], split_complex(x)[1], int(split_complex(x)[5]), int(split_complex(x)[6]))). \
        filter(lambda x : x[0] != '' and x[2] != 0 and x[3] != 0 and int(x[0]) >= 2000). \
        map(lambda x : (x[0], (x[1], (x[3]-x[2])*100/x[2]))). \
        reduceByKey(lambda x,y: x if x[1]>y[1] else y). \
        sortByKey(). \
        map(lambda x : (x[0], x[1][0], x[1][1])).collect()


''''

Implementation without split_complex(x), which gives the same results

res = \
        sc.textFile("hdfs://master:9000/movies/movies.csv"). \
        map(lambda x : (x.split(",")[-5][0:4], x.split(',')[1], int(x.split(",")[-3]), int(x.split(",")[-2]))). \
        filter(lambda x : x[0] != '' and x[2] != 0 and x[3] != 0 and int(x[0]) >= 2000). \
        map(lambda x : (x[0], (x[1], (x[3]-x[2])*100/x[2]))). \
        reduceByKey(lambda x,y: x if x[1]>y[1] else y). \
        sortByKey(). \
        map(lambda x : (x[0], x[1][0], x[1][1])).collect()
'''

for i in res:
        print(i)
