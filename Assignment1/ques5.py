# 5. Movie recommendation using Spark dataframes.

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


filepath1 = '/home/sunbeam/pracBigData/movies/movies.csv'

movies = spark.read \
    .option('header','true') \
    .option('inferSchema','true') \
    .csv(filepath1)

# movies.printSchema()
# movies.show(n=20,truncate=False)



result = ratings.alias('r2') \
        .join(ratings.alias('r1'), col('r1.movieId')<col('r2.movieId'), 'inner') \
        .select(col('r1.movieId'), col('r2.movieId'), col('r1.rating'),col('r2.rating')) \
        .groupBy(col('r1.movieId'),col('r2.movieId')) \
        .agg(corr(col('r2.rating'),col('r1.rating')))


result.printSchema()
result.show()


spark.stop()
