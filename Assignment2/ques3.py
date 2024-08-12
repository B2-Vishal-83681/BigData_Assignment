from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName('ques3')\
    .getOrCreate()

filepath = '/home/sunbeam/pracBigData/movies/ratings.csv'

ratings = spark.read\
    .option('header','true')\
    .option('inferSchema','true')\
    .csv(filepath)

# ratings.printSchema()
# ratings.show()

ratings.createOrReplaceTempView('v_ratings')

no_ratings = spark.sql("SELECT month(from_unixtime(timestamp)) yr,count(movieId) cn from v_ratings group by month(from_unixtime(timestamp)) ")

no_ratings.printSchema()
no_ratings.show()

spark.stop()
print('exit...')
