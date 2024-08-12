from pyspark.sql import SparkSession
from pyspark.sql.functions import col, corr
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator


# Initialize Spark session
spark = SparkSession.builder \
    .appName("MovieRecommendation") \
    .getOrCreate()

# File paths
ratings_filepath = '/home/sunbeam/pracBigData/movies/ratings.csv'
movies_filepath = '/home/sunbeam/pracBigData/movies/movies.csv'

# Load ratings and movies data
ratings = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(ratings_filepath)

movies = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(movies_filepath)

# Optionally, explore ratings and movies data
# ratings.printSchema()
# ratings.show(n=10)
# movies.printSchema()
# movies.show(n=20, truncate=False)

# Example of calculating correlations (optional)
result = ratings.alias('r2') \
        .join(ratings.alias('r1'), col('r1.movieId') < col('r2.movieId'), 'inner') \
        .select(col('r1.movieId'), col('r2.movieId'), col('r1.rating'), col('r2.rating')) \
        .groupBy(col('r1.movieId'), col('r2.movieId')) \
        .agg(corr(col('r2.rating'), col('r1.rating')))

result.printSchema()
result.show()

# Stop Spark session
spark.stop()
