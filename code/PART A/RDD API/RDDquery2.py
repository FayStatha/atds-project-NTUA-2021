from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("query2-rdd").getOrCreate()

sc = spark.sparkContext

users = \
	sc.textFile("hdfs://master:9000/movies/ratings.csv"). \
	map(lambda x : (x.split(",")[0], (float(x.split(",")[2]), 1))). \
	reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1]) ). \
	map(lambda x : (x[0], x[1][0] / x[1][1])). \
	filter( lambda x : x[1] > 3).count()

all_users = \
	sc.textFile("hdfs://master:9000/movies/ratings.csv"). \
	map(lambda x : x.split(",")[0]).distinct().count()

res = users/all_users * 100

print(str(res) + '%')
