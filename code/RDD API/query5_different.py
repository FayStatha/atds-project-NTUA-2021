from pyspark.sql import SparkSession
from io import StringIO
import csv

spark = SparkSession.builder.appName("query5-rdd").getOrCreate()

sc = spark.sparkContext

# Einai h arxiki ulopoihsh pou ekana gia to Q5, h opoia edeixne gia ton ekastote 
# xrhsth me tis perissoteres kritikes ana kathgoria genika poia einai h agaphmenh 
# tou kai poia h ligotero agaphmenh tou tainia
# To parathetw aplws gia plhrothta !

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
        map(lambda x : (x[1][0], (x[0], x[1][1])))

movie_popularity = \
        sc.textFile("hdfs://master:9000/movies/movies.csv"). \
        map(lambda x : (split_complex(x)[0], (split_complex(x)[1] , float(split_complex(x)[7]))))

user_movie_ratings = \
        sc.textFile("hdfs://master:9000/movies/ratings.csv"). \
        map(lambda x : (x.split(",")[1], (x.split(",")[0], float(x.split(",")[2])))). \
        join(movie_popularity). \
        map(lambda x : (x[1][0][0], (x[1][1][0], x[1][0][1], x[1][1][1])))

user_fav_movie = \
        user_movie_ratings.reduceByKey(lambda x, y: x if x[1] > y[1] or (x[1] == y[1] and x[2] > y[2]) else y)

user_worst_movie = \
        user_movie_ratings.reduceByKey(lambda x, y: x if x[1] < y[1] or (x[1] == y[1] and x[2] > y[2]) else y)

user_fav_worst_movie = \
        user_fav_movie.join(user_worst_movie). \
        map(lambda x : (x[0], (x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1])))

res = category_ratings.join(user_fav_worst_movie).collect()
       map(lambda x : (x[1][0][0], x[0], x[1][0][1], x[1][1][0], x[1][1][1], x[1][1][2], x[1][1][3])). \
        sortBy(lambda x: x[1], ascending = True).collect()

for i in res:
        print(i)

