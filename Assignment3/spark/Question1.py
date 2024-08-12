#Wordcount using Spark Dataframes and nd top 10 words (except stopwords)
# WITH words AS
# (SELECT EXPLODE(SPLIT(LOWER(line), '[^a-z0-9]')) word FROM wordfile)
# SELECT word, COUNT(word) cnt FROM words
# WHERE word NOT IN ('', 'the', 'a', 'an', 'or', 'of', 'and', 'to', 'any', 'and', 'for', 'in', 'by', 'from', 'that', 'under', 'over', 'this', 'such', 'as', 'shall', 'your', 'my')
# GROUP BY word
# ORDER BY cnt DESC
# LIMIT 20;

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
filepath = '/home/sunbeam/spark-3.3.1-bin-hadoop3/LICENSE'
textss = spark.read \
        .option('header','false') \
        .text(filepath)


# test data frame
textss.printSchema()
textss.show(truncate=False)

result = textss\
        .select(explode(split("value",'[^a-z0-9]')).alias('words'))\
        .groupBy('words').count()\
        .where('words not in ("the","","a","and")')\
        .orderBy(desc('count'))


result.printSchema()
result.show(truncate=False)
spark.stop()
