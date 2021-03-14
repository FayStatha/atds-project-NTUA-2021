from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.appName("Query3_DFAPI").getOrCreate()

input_format = sys.argv[1]

if input_format == 'parquet':
        movie_genres = spark.read.parquet("hdfs://master:9000/movies/movie_genres.parquet") #returns a dataframe that contains ratings table 
        movies = spark.read.parquet("hdfs://master:9000/movies/movies.parquet") #returns a dataframe that contains ratings table 
        ratings = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet") #returns a dataframe that contains rating table 
else:
        movies = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/movies/movies.csv")
        movie_genres = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/movies/movie_genres.csv")
        ratings = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/movies/ratings.csv")

movies.registerTempTable("movies")
ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")

sqlString = "select a.Genre as Movie_Genre, avg(b.Rating) as Avg_Rating, count(b.ID) as No_of_movies \
             from \
             (select distinct _c1 as Genre, _c0 as aID from movie_genres)a \
             inner join ( \
             select distinct _c1 as ID, avg(_c2) as Rating from ratings where _c2 is not null group by _c1\
             )b \
             on a.aID = b.ID \
             group by Genre\
             order by Genre"

res = spark.sql(sqlString)
res.show()