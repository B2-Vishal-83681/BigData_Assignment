#Find max sal per dept per job in emp.csv le.
#select deptno,max(sal) from emp group by deptno order by max(sal) limit 1;
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
            .getOrCreate()
emp_filepath = '/home/sunbeam/dbdadata1/BigData/data/emp.csv'
emp = spark.read \
        .option('header', 'false') \
        .option('inferSchema', 'true') \
        .csv(emp_filepath)

emp.show(truncate=False)

result = emp \
        .select('_c7','_c5','_c1')\
        .groupBy('_c7').max('_c5') \
        .withColumnRenamed('max(_c5)', 'maxsal')\
        .orderBy('maxsal')



result.show(truncate=False)

spark.stop()
