#Count number of movie ratings per year.
#SELECT YEAR(rtime) yr, COUNT(*) cnt FROM ratings
#GROUP BY YEAR(rtime);
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
            .getOrCreate()
ratings_filepath = '/home/sunbeam/dbdadata1/BigData/data/movies/ratings.csv'
ratings = spark.read \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .csv(ratings_filepath)

ratings.show(truncate=False)
ratings = ratings.withColumn("timestamp", ratings["timestamp"].cast("timestamp"))
# Assuming 'ratings' is your dataframe
ratings_per_year = ratings\
    .withColumn("year", year(col("timestamp")))

# Count the number of ratings per year
ratings_count_per_year = ratings_per_year\
    .groupBy("year")\
    .agg(count("*").alias("rating_count"))\
    .orderBy("year")

# Show the result
ratings_count_per_year.show()

# Stop the Spark session
spark.stop()

