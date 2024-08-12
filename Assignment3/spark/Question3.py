from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

# Load emp.csv and dept.csv
emp_filepath = '/home/sunbeam/dbdadata1/BigData/data/emp.csv'
emp = spark.read.option('header', 'false').option('inferSchema', 'true').csv(emp_filepath)

# Provide proper column names for emp DataFrame
emp = emp.toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

dept_filepath = '/home/lenovo/dbdadata1/BigData/data/dept.csv'
dept = spark.read.option('header', 'false').option('inferSchema', 'true').csv(dept_filepath)

# Provide proper column names for dept DataFrame
dept = dept.toDF("deptno", "dname", "loc")

# Convert salary column to numeric
emp = emp.withColumn("sal", emp["sal"].cast("double"))

# Join emp and dept DataFrames based on deptno
joined_df = emp.join(dept, emp["deptno"] == dept["deptno"], "inner")

# Calculate total salary per department
deptwise_total_sal = joined_df.groupBy(dept["dname"]).agg(sum("sal").alias("total_salary"))

# Show the result
deptwise_total_sal.show()

# Stop the Spark session
spark.stop()
