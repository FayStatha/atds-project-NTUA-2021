from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.appName("Query5-SparkSQL").getOrCreate()

# The user must give the input format (csv || parquet)

# For example:
# spark-submit SparkSqlquery5.py csv
# to read csv file

input_format = sys.argv[1]

if input_format == 'parquet':
        movie_genres = spark.read.parquet("hdfs://master:9000/movies/movie_genres.parquet") 
        movies = spark.read.parquet("hdfs://master:9000/movies/movies.parquet") 
        ratings = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet")  
else:
        movies = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/movies/movies.csv")
        movie_genres = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/movies/movie_genres.csv")
        ratings = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/movies/ratings.csv")

movies.registerTempTable("movies")
ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")


sqlString = \
        "select b.Genre, first(b.UserId) as UserId, first(b.SumOfRatings) as SumOfRatings, first(b.Name) as FavoriteName, first(b.Rating) as FavoriteRating, \
                first(b.WorstName) as WorstName, first(b.WorstRating) as WorstRating\
        from\
                (select a.Genre, a.UserId, a.WorstRating, a.SumOfRatings, a.Name, a.Rating, a.FavRating, max(SumOfRatings) over (PARTITION BY Genre) as MaxSum, a.FavName, a.WorstName\
                from\
                        (select g._c1 as Genre, r._c0 as UserId, m._c1 as Name, r._c2 as Rating,\
                                count(r._c2) over (PARTITION BY g._c1, r._c0) as SumOfRatings, \
                                first(r._c2) over (PARTITION BY g._c1, r._c0 order by r._c2 DESC, m._c7 DESC) as FavRating,\
                                first(m._c1) over (PARTITION BY g._c1, r._c0 order by r._c2 DESC, m._c7 DESC) as FavName, \
                                first(r._c2) over (PARTITION BY g._c1, r._c0 order by r._c2 ASC, m._c7 DESC) as WorstRating,\
                                first(m._c1) over (PARTITION BY g._c1, r._c0 order by r._c2 ASC, m._c7 DESC) as WorstName\
                        from movie_genres as g inner join ratings as r on g._c0 = r._c1 inner join movies as m on m._c0 = r._c1\
                        )a\
                )b\
        where b.SumOfRatings = b.MaxSum and b.FavRating = b.Rating\
        group by b.Genre\
        order by b.Genre ASC"


res = spark.sql(sqlString)
res.show(26)
