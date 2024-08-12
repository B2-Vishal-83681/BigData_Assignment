## SELECT m.title, COUNT(r.rating) count FROM movies m
##  INNER JOIN ratings r ON m.movieId = r.movieId
##  GROUP BY m.title
##  ORDER BY count DESC
##  LIMIT 10;

#import packages

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .getOrCreate()

#load data and create dataframe
filepath1 = '/home/sunbeam/pracBigData/movies/movies.csv'

movies = spark.read \
    .option('header','true') \
    .option('inferSchema','true') \
    .csv(filepath1)


#test dataframe
# movies.printSchema()
# movies.show(n=5,truncate=False)

filepath2 = '/home/sunbeam/pracBigData/movies/ratings.csv'
ratings = spark.read \
    .option('header','true') \
    .option('inferSchema','true') \
    .csv(filepath2)

# ratings.printSchema()
# ratings.show(truncate=False)

#perform operations on dataframes
result = ratings \
        .groupBy('movieId').count() \
        .join(movies,[ratings.movieId == movies.movieId], 'inner') \
        .select('title','count') \
        .orderBy(desc('count')) \
        .limit(10)
        


#display result
result.printSchema()
result.show(truncate=False)

spark.stop()
