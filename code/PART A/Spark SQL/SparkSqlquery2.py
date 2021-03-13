from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("query2-SparkSql").getOrCreate()

# The user must give the input format (csv || parquet)

# For example:
# spark-submit SparkSqlquery2-csv.py csv
# to read csv file

input_format = sys.argv[1]

if input_format == 'csv':
        df = spark.read.format('csv').options(header = 'false', inferSchema = 'true'). \
                load("hdfs://master:9000/movies/ratings.csv")
else:
        df = spark.read.parquet("hdfs://master:9000/movies/ratings.parquet")

df.registerTempTable("ratings")

all_users = "select * \
                    from (select distinct _c0 from ratings)"

users = "select * \
         from ( \
         select _c0 \
         from ratings \
         group by _c0 \
         having AVG(_c2 ) > 3 )"

res1 = spark.sql(users)
num = res1.count()

res = spark.sql(all_users)
den = res.count()

percentage = 100 * (num / den)
print(str(percentage) + "%")
