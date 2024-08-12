# 3. Find deptwise total sal from emp.csv and dept.csv. Print dname and total sal.


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



filepath1 = '/home/sunbeam/BigData/data/emp.csv'

emp = spark.read \
    .schema(schema) \
    .csv(filepath1)


filepath2 = '/home/sunbeam/BigData/data/dept.csv'

schema2 = StructType() \
    .add('deptno',IntegerType()) \
    .add('dname',StringType()) \
    .add('city',StringType())

depts = spark.read \
    .schema(schema2) \
    .csv(filepath2)

# depts.printSchema()
# depts.show()

# emp.printSchema()
# emp.show(truncate=False)


# result = emp \
#         .groupBy('deptno').sum('sal') \
#         .join(depts,[emp.deptno==depts.deptno],'inner') \
#         .select('dname','sum(sal)')


result = emp \
        .join(depts,[emp.deptno==depts.deptno],'inner') \
        .select('dname','sal') \
        .groupBy('dname').sum('sal')




result.printSchema()
result.show()


spark.stop()
