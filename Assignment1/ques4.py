# 4. Count number of movie ratings per year.
# select count(rating),year(rtime) from tabl group by year(rtime)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .getOrCreate()


filepath = '/home/sunbeam/pracBigData/movies/ratings.csv'

ratings = spark.read \
    .option('header','true') \
    .option('inferSchema','true') \
    .csv(filepath)

# ratings.printSchema()
# ratings.show(n=10)

result = ratings \
    .select(year(from_unixtime("timestamp")).alias('year'),'rating') \
    .groupBy('year') \
    .count()


result.printSchema()
result.show()


spark.stop()
