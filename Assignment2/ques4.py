from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName('ques3')\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()

filepath = '/home/sunbeam/pracBigData/movies/ratings.csv'
filepath2='/home/sunbeam/pracBigData/movies/movies.csv'
ratings = spark.read\
    .option('header','true')\
    .option('inferSchema','true')\
    .csv(filepath)

# ratings.printSchema()
# ratings.show()

movies = spark.read\
    .option('header','true')\
    .option('inferSchema','true')\
    .csv(filepath2)

# movies.printSchema()
# movies.show(n=10,truncate=False)

movies.createOrReplaceTempView('v_movies')
ratings.createOrReplaceTempView('v_ratings')

corrm = spark.sql("select t1.movieId m1, t2.movieId m2, t1.rating r1, t2.rating r2 from v_ratings t1 inner join v_ratings t2 on t1.userId=t2.userId and t1.movieId>t2.movieId")

corrm.createOrReplaceTempView('v_corrm')

corrt=spark.sql("select m1, m2, corr(r1,r2) corr1 from v_corrm group by m1,m2")

corrt.createOrReplaceTempView('v_corrt')

# recomm = spark.sql("select ct.m1,(select x.title from v_movies x where x.movieId=ct.m1) title1, ct.m2, (select y.title from v_movies y where y.movieId=ct.m2) title2 ,ct.corr1 from v_corrt ct where ct.m1=225 and ct.corr1>0.5")


recomm = spark.sql("""
    SELECT 
        ct.m1,
        x.title AS title1,
        ct.m2,
        y.title AS title2,
        ct.corr1
    FROM 
        v_corrt ct
    JOIN 
        v_movies x 
    ON 
        ct.m1 = x.movieId
    JOIN 
        v_movies y 
    ON 
        ct.m2 = y.movieId
    WHERE 
        ct.m1 = 225 
        AND ct.corr1 > 0.5
""")
recomm.show(n=10,truncate=False)


spark.stop()
print('exit...')
