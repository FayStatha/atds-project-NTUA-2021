from pyspark.sql import SparkSession
from io import StringIO
import csv, sys, time

spark = SparkSession.builder.appName("broadcast-join").getOrCreate()

sc = spark.sparkContext

def join(x):
    result = []
    for v in table_broad.value:
        if v[key1_index] == x[0]:
            new = (x[0], tuple(x[1]), tuple(v[:key1_index] + v[key1_index+1:]))
            result.append(new)
    return result

# The user must give the file path of each file as input with the index of the key for each one

# For example:
# spark-submit repartition_join.py "hdfs://master:9000/movies/movie-genres-100.csv" 0 "hdfs://master:9000/movies/ratings.csv" 1
# to join movie-genres-100 with ratings on movie id

file1 = sys.argv[1]

key1_index = int(sys.argv[2])

file2 = sys.argv[3]

key2_index = int(sys.argv[4])

table1 = \
        sc.textFile(file1). \
        map(lambda x : x.split(',')).collect()

table2 = \
        sc.textFile(file2). \
        map(lambda x : (x.split(','))). \
        map(lambda x : (x.pop(key2_index), x))

start = time.time()

table_broad = sc.broadcast(table1)

joined_table = table2. \
        flatMap(lambda x : join(x)).collect()

end = time.time()

for i in joined_table:
        print(i)

print("\n\n Time for join: %.4f seconds\n\n"%(end-start))
