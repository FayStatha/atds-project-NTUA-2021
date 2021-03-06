from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("query5-rdd").getOrCreate()

sc = spark.sparkContext

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

genres = \
        sc.textFile("hdfs://master:9000/movies/movie_genres.csv"). \
        map(lambda x : (x.split(",")[0], x.split(",")[1]))

ratings = \
        sc.textFile("hdfs://master:9000/movies/ratings.csv"). \
        map(lambda x : (x.split(",")[1], x.split(",")[0]))

category_ratings = genres.join(ratings). \
        map(lambda x : (x[1] , 1)). \
        reduceByKey(lambda x, y: x+y). \
        map(lambda x : (x[0][0], (x[0][1], x[1]))). \
        reduceByKey(lambda x, y: x if x[1]>y[1] else y). \
        map(lambda x : ((x[0] ,x[1][0]), x[1][1]))

movie_popularity = \
        sc.textFile("hdfs://master:9000/movies/movies.csv"). \
        map(lambda x : (split_complex(x)[0], (split_complex(x)[1] , float(split_complex(x)[7]))))

user_movie_ratings = \
        sc.textFile("hdfs://master:9000/movies/ratings.csv"). \
        map(lambda x : (x.split(",")[1], (x.split(",")[0], float(x.split(",")[2])))). \
        join(movie_popularity). \
        map(lambda x : (x[0], (x[1][0][0], x[1][1][0], x[1][0][1], x[1][1][1]))). \
        join(genres). \
        map(lambda x : ((x[1][1], x[1][0][0]), (x[1][0][1], x[1][0][2], x[1][0][3])))

user_fav_movie = \
        user_movie_ratings.reduceByKey(lambda x, y: x if x[1] > y[1] or (x[1] == y[1] and x[2] > y[2]) else y)

user_worst_movie = \
        user_movie_ratings.reduceByKey(lambda x, y: x if x[1] < y[1] or (x[1] == y[1] and x[2] > y[2]) else y)

res = \
        user_fav_movie.join(user_worst_movie). \
        map(lambda x : (x[0], (x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1]))). \
        join(category_ratings). \
        map(lambda x : (x[0][0], x[0][1], x[1][1], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3])). \
        sortBy(lambda x : x[0], ascending = True).collect()

for i in res:
        print(i)

