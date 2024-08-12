# 2. Find max sal per dept per job in emp.csv file.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .getOrCreate()

schema = StructType() \
    .add('empno',IntegerType()) \
    .add('ename',StringType()) \
    .add('job',StringType()) \
    .add('mgr', IntegerType()) \
    .add('hire', DateType()) \
    .add('sal', DoubleType()) \
    .add('comm', DoubleType()) \
    .add('deptno', IntegerType())



filepath = '/home/sunbeam/BigData/data/emp.csv'

emp = spark.read \
    .schema(schema) \
    .csv(filepath)



# emp.printSchema()
# emp.show(truncate=False)

result = emp \
        .select('deptno','job', 'sal') \
        .groupBy('deptno','job').max('sal') \
        .withColumnRenamed('max(sal)','max_sal')



result.printSchema()
result.show()


spark.stop()
