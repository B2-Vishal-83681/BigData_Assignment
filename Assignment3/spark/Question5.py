

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
            .getOrCreate()
def question5():
    movie_path = '/home/sunbeam/dbdadata1/BigData/data/movies/movies.csv'
    rating_path = '/home/sunbeam/dbdadata1/BigData/data/movies/ratings.csv'

    movies = spark.read.option('header', 'true').option('inferSchema', 'true').csv(movie_path)
    ratings = spark.read.option('header', 'true').option('inferSchema', 'true').csv(rating_path)

    result1 = ratings.alias("a").join(ratings.alias("b"),
                                      (col('a.userId') == col('b.userId')) & (col('a.movieId') < col('b.movieId')),
                                      'inner')
    result2 = result1.select(col('a.movieId').alias('m1'), col('a.rating').alias('r1'), col('b.movieId').alias('m2'),
                             col('b.rating').alias('r2'))
    result3 = result2.groupBy('m1', 'm2').agg(corr('r1', 'r2').alias('correl')).filter(col('correl') > 0.7)

    result3.show()

    recommend = result3.join(movies.alias("t1"), result3.m1 == col('t1.movieId'), 'inner') \
        .select('m1', col('t1.title').alias('movie1'), 'm2', 'correl')

    # Join again with movies DataFrame to get the name for movieId2 (m2)
    recommend = recommend.join(movies.alias("t2"), recommend.m2 == col('t2.movieId'), 'inner') \
        .select('movie1', col('t2.title').alias('movie2'), 'correl')

    recommend.show()
    spark.stop()
question5()
