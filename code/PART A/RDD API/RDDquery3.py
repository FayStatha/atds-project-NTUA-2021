from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("query3-rdd").getOrCreate()

sc = spark.sparkContext

genres = \
        sc.textFile("hdfs://master:9000/movies/movie_genres.csv"). \
        map(lambda x : (x.split(",")[0], x.split(",")[1]))


ratings = \
        sc.textFile("hdfs://master:9000/movies/ratings.csv"). \
        map(lambda x : (x.split(",")[1], float(x.split(",")[2])))

res = genres.join(ratings). \
        map(lambda x : ((x[1][0], x[0]),(x[1][1], 1))). \
        reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1])). \
        map(lambda x : (x[0][0], (x[1][0] / x[1][1], 1))). \
        reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1])). \
        sortByKey(). \
        map(lambda x : (x[0], x[1][0] / x[1][1], x[1][1])).collect()

for i in res:
	print(i)
