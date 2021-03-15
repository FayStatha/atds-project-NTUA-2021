from pyspark.sql import SparkSession

import datetime, sys
from pyspark.sql.functions import year, month, dayofmonth

spark = SparkSession.builder.appName("query1-SparkSQL").getOrCreate()

sc = spark.sparkContext

# The user must give the input format (csv || parquet)

# For example:
# spark-submit SparkSqlquery1.py csv
# to read csv file

input_format = sys.argv[1]

if input_format == 'csv':
        movies = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/movies/movies.csv")
else:
        movies = spark.read.parquet("hdfs://master:9000/movies/movies.parquet")
movies.registerTempTable("movies")

sqlString = \
sqlString = \
        "select year(_c3) as Year, (_c1) as MovieName, maxB as Profit\
        from \
        (select *, max(100*(_c6-_c5)/(_c5)) \
        OVER (PARTITION BY year(_c3)) \
        AS maxB \
        from movies) M \
        where _c6 != 0 and _c5 != 0 and (100*(_c6-_c5)/(_c5)) = maxB and year(_c3) > 1999\
        order by year(_c3) ASC"

res = spark.sql(sqlString)

res.show()
