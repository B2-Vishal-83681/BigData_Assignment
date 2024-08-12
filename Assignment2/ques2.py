from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName('ques2')\
    .getOrCreate()


dbUrl = 'jdbc:mysql://localhost:3306/practice'
dbDriver = 'com.mysql.cj.jdbc.Driver'
dbUser = 'root'
dbPasswd = 'manager'
dbTable = 'ncdcsummary123'
result=spark.read\
    .option('url', dbUrl)\
    .option('driver', dbDriver)\
    .option('user', dbUser) \
    .option('password', dbPasswd) \
    .option('dbtable', dbTable) \
    .format('jdbc') \
    .load()

# result.show()

avg_temp=result\
    .select('yr','temp')\
    .groupBy('yr').avg('temp')\
    .withColumnRenamed('avg(temp)','avg_temp')\
    .orderBy(desc('avg_temp'))

avg_temp.printSchema()
avg_temp.show()

spark.stop()
print('exit...')