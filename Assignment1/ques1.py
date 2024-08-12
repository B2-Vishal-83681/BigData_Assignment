#1. Wordcount using Spark Dataframes and nd top 10 words (except stopwords).

#import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .getOrCreate()

filepath= '/home/sunbeam/hadoop-3.3.2/LICENSE.txt'

words = spark.read.text(filepath)


# words.printSchema()
# words.show(n=10,truncate=False)

result = words \
        .select(explode(split(lower('value'),'[^a-z0-9]')).alias('w')) \
        .where("w not in ('','the','or','of','and','any','by','that','a','in','for','to')") \
        .groupBy('w').count() \
        .orderBy(desc('count'))




result.printSchema()
result.show(truncate=False)

spark.stop()
