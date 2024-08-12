from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName('ques5')\
    .config("spark.driver.memory", "4g")\
.config('spark.sql.legacy.timeParserPolicy', 'LEGACY')\
    .getOrCreate()

filepath = '/home/sunbeam/BigData/data/Fire_Service_Calls_Sample.csv'


schema = """
call_number BIGINT, unit_id STRING, incident_number BIGINT, call_type STRING, call_date string, watch_date string, received_dttm string, entry_dttm string, dispatch_dttm string, response_dttm string, onscene_dttm string, transport_dttm string, hospital_dttm string, final_disp string, available_dttm string, address string, city string, zipcode_inci BIGINT, battalion string, station_area int, box int, orig_prio string, prio string, final_prio int, als_unit string, call_type_grp string, number_of_alarms int, unit_type string, seq_call_disp int, fire_prev_dist int, supervisor_dist int, neighbor string, row_id string, case_loc string, data_as_of string, data_loaded_at string, analysis_neighbor int
"""
fire_calls_staging = spark.read.option('header', 'true').schema(schema).csv(filepath).cache()
fire_calls_staging.createOrReplaceTempView('v_fire_staging')

fire_calls_staging.show(n=10)

fire_calls = spark.sql("""
               SELECT 
        call_number, 
        unit_id, 
        incident_number, 
        call_type, 
        to_date(from_unixtime(unix_timestamp(call_date, 'MM/dd/yy'))) AS call_date,
        to_date(from_unixtime(unix_timestamp(watch_date, 'MM/dd/yy'))) AS watch_date,
        from_unixtime(unix_timestamp(received_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS received_dttm,
        from_unixtime(unix_timestamp(entry_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS entry_dttm,
        from_unixtime(unix_timestamp(dispatch_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS dispatch_dttm,
        from_unixtime(unix_timestamp(response_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS response_dttm,
        from_unixtime(unix_timestamp(onscene_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS onscene_dttm,
        from_unixtime(unix_timestamp(transport_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS transport_dttm,
        from_unixtime(unix_timestamp(hospital_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS hospital_dttm,
        final_disp, 
        from_unixtime(unix_timestamp(available_dttm, 'MM/dd/yyyy hh:mm:ss a')) AS available_dttm,
        address, 
        city, 
        cast(zipcode_inci as INT) AS zipcode_inci, 
        battalion, 
        cast(station_area as INT) AS station_area,
        cast(box as INT) AS box, 
        orig_prio, 
        prio, 
        final_prio, 
        als_unit, 
        call_type_grp, 
        cast(number_of_alarms as INT) AS number_of_alarms,
        unit_type, 
        cast(seq_call_disp as INT) AS seq_call_disp, 
        cast(fire_prev_dist as INT) AS fire_prev_dist, 
        cast(supervisor_dist as INT) AS supervisor_dist,
        neighbor, 
        row_id, 
        case_loc, 
        data_as_of, 
        from_unixtime(unix_timestamp(data_loaded_at, 'MM/dd/yyyy hh:mm:ss a')) AS data_loaded_at,
        cast(analysis_neighbor as INT) AS analysis_neighbor 
    FROM v_fire_staging
                        """)

fire_calls.show(n=10)

# 3. Find out all responses for delayed times greater than 5 mins?
q3_1 = fire_calls.selectExpr('unix_timestamp(response_dttm)-unix_timestamp(received_dttm)')\
        .withColumnRenamed('(unix_timestamp(response_dttm, yyyy-MM-dd HH:mm:ss) - unix_timestamp(received_dttm, yyyy-MM-dd HH:mm:ss))', 'delay')
q3 = q3_1.select('delay').where('delay > 300').agg(count('delay'))


# 6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

q6 = fire_calls\
    .select('neighbor')\
    .where('city = "San Francisco" and zipcode_inci in (94102, 94103)')\
    .distinct()

# q6.show()

# 7. What was the sum of all calls, average, min, and max of the call response times?

q7 = fire_calls\
    .agg(sum(from_unixtime('response_dttm')-from_unixtime('received_dttm')).alias('sum_time'),
         avg(from_unixtime('response_dttm')-from_unixtime('received_dttm')).alias('avg_time'),
         min(from_unixtime('response_dttm')-from_unixtime('received_dttm')).alias('min_time'),
         max(from_unixtime('response_dttm')-from_unixtime('received_dttm')).alias('max_time')
         )
# q7.show()

# 8. How many distinct years of data are in the CSV file?

q8 = fire_calls\
    .select(count_distinct(year('call_date')))

# q8.show()

# 9. What week of the year in 2018 had the most fire calls?
q9 = fire_calls.select(weekofyear('call_date').alias('week'))\
                    .where('year(call_date)=2018 and call_type like "%Fire%"')\
                    .groupBy('week')\
                    .agg(count('week').alias('cnt'))\
                    .orderBy(desc('cnt')).limit(1)

# q9.show(n=10)

# 10. What neighborhoods in San Francisco had the worst response time in 2018?
q10 = fire_calls.select('neighbor',(unix_timestamp('response_dttm')-unix_timestamp('received_dttm')).alias('delay'))\
                        .where('city="San Francisco" and year(call_date)=2018')\
                        .groupBy('neighbor').avg('delay')\
                        .orderBy(desc('avg(delay)'))

# q10.show(n=10)

spark.stop()
print('exit...')
