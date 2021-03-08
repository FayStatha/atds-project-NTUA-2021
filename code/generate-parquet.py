# This script is used to generate and save .parquet files
# from .csv files, to use them as an input to SparkSQL query scripts

from pyspark.sql import SparkSession
from io import StringIO
import csv, sys

spark = SparkSession.builder.appName("generate-parquet").getOrCreate()

# The user must give the path of the csv file in hdfs
# and also the path in hdfs to put the new file

# For example: 
# spark-submit generate-parquet.py "hdfs://master:9000/movies/ratings.csv"  "hdfs://master:9000/movies/ratings.parquet"


path = sys.argv[1]

new_path = sys.argv[2]

df = spark.read.format('csv').options(header = 'false', inferSchema = 'true').load(path)

df.write.parquet(new_path)
