from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName('ques1')\
    .getOrCreate()

filepath = '/home/sunbeam/pracBigData/ncdc'

ncdc=spark.read\
    .text(filepath)


regex = r'^.{15}([0-9]{4}).{68}([-\+][0-9]{4})([0-9]).*$'

temps = ncdc\
    .select(
    regexp_extract('value',regex,1).cast('int').alias('yr'),
regexp_extract('value',regex,2).cast('int').alias('temp'),
regexp_extract('value',regex,3).cast('int').alias('quality'),
)

result=temps \
    .where('quality IN (0,1,2,4,5,9) AND temp != 9999')

result.printSchema()
result.show()


dbUrl = 'jdbc:mysql://localhost:3306/practice'
dbDriver = 'com.mysql.cj.jdbc.Driver'
dbUser = 'root'
dbPasswd = 'manager'
dbTable = 'ncdcsummary123'
result.write\
    .option('url', dbUrl)\
    .option('driver', dbDriver)\
    .option('user', dbUser) \
    .option('password', dbPasswd) \
    .option('dbtable', dbTable) \
    .mode('OVERWRITE') \
    .format('jdbc') \
    .save()

spark.stop()
print('exit...')

#  select * from ncdcsummary123 order by temp limit 10;
# +------+------+---------+
# | yr   | temp | quality |
# +------+------+---------+
# | 1917 | -478 |       1 |
# | 1917 | -472 |       1 |
# | 1917 | -456 |       1 |
# | 1918 | -450 |       1 |
# | 1918 | -444 |       1 |
# | 1917 | -433 |       1 |
# | 1918 | -428 |       1 |
# | 1919 | -428 |       1 |
# | 1918 | -422 |       1 |
# | 1919 | -417 |       1 |
# +------+------+---------+
# 10 rows in set (0.04 sec)
