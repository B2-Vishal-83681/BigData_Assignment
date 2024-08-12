from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .appName('ques5')\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()

filepath = '/home/sunbeam/BigData/data/Fire_Service_Calls_Sample.csv'


schema = """
call_number BIGINT, unit_id STRING, incident_number BIGINT, call_type STRING, call_date string, watch_date string, received_dttm string, entry_dttm string, dispatch_dttm string, response_dttm string, onscene_dttm string, transport_dttm string, hospital_dttm string, final_disp string, available_dttm string, address string, city string, zipcode_inci BIGINT, battalion string, station_area int, box int, orig_prio string, prio string, final_prio int, als_unit string, call_type_grp string, number_of_alarms int, unit_type string, seq_call_disp int, fire_prev_dist int, supervisor_dist int, neighbor string, row_id string, case_loc string, data_as_of string, data_loaded_at string, analysis_neighbor int
"""
fire_calls_staging = spark.read.option('header', 'true').schema(schema).csv(filepath).cache()
fire_calls_staging.createOrReplaceTempView('v_fire_staging')

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

# . Execute following queries on fire dataset.
# 1. How many distinct types of calls were made to the fire department?

# no_distinct_calls = fire\
#     .select('Call Number').distinct()\
#     .count()

# print(no_distinct_calls)

# 2. What are distinct types of calls made to the fire department?

# distinct_calls = fire\
#         .select('Call Type').distinct()

# distinct_calls.show(n=10,truncate=False)

# 3. Find out all responses for delayed times greater than 5 mins?


# result1 = fire\
#         .selectExpr('from_unixtime("Response DtTm")-from_unixtime("Received DtTm")').alias('delay')\
#     .where('delay > 5')
#
# result1.show(n=10)

# 4. What were the most common call types?

# common_calls = fire \
#         .groupBy('Call Type').count()\
#         .orderBy(desc('count'))
#
# common_calls.show(n=5)

# 5. What zip codes accounted for the most common calls?

# common_zipCode = fire\
#         .groupBy('Zipcode of Incident','Call Type').count() \
#         .orderBy(desc('count'))
#
# common_zipCode.show(n=10)

# 6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

# result = fire \
#         .select('Neighborhooods - Analysis Boundaries')\
#         .where("'Zipcode of Incident' in (94102,94103) and City in (SF)")
#
# result.show(n=10)

# 7. What was the sum of all calls, average, min, and max of the call response times?
# 8. How many distinct years of data are in the CSV le?
# 9. What week of the year in 2018 had the most re calls?
# 10. What neighborhoods in San Francisco had the worst response time in 2018?

spark.stop()
print('exit...')
