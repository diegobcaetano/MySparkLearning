from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType

# approx_count_distinct() ,
# (collect_list() , 
#  collect_set() ,
#  corr() ,
#  count() ,
#  countDistinct() ,)
# covar_ pop() , covar_samp() ,
# first() ,
# grouping() ,
# grouping_id() ,
# kurtosis() ,
# last() , max() ,
# mean() , min() ,
# skewness() ,
# stddev() ,
# stddev_ pop() ,
# stddev_samp() ,
# sum() ,
# sumDistinct() ,
# var_ pop() ,
# var_samp() ,
# variance()

BASEPATH = "../../data/government"

spark = (SparkSession
         .builder
         .appName("Analytics Exercises")
         .master("local[3]")
         .getOrCreate())

schools_df = (spark
              .read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(f"{BASEPATH}/school/2018-2021_Daily_Attendance_by_School_20241003.csv"))

schools_df = schools_df.withColumnRenamed('School DBN', 'school_id')

print(f"Number of schools: {schools_df.select('school_id').distinct().count()}")

# WHAT IS THE AVERAGE ENROLLMENT FOR YEAR?

(schools_df
 .groupBy('SchoolYear')
 .agg(f.round(f.avg('Enrolled'), 2).alias('enrollment_avg'))
 .orderBy(f.asc('SchoolYear'))
 .show()
 )

# WHAT IS THE RANKING OF ENROLLMENT PER YEAR?

enrolled_window = Window.partitionBy('SchoolYear').orderBy(f.desc('sum(Enrolled)'))

(schools_df
 .groupBy('SchoolYear', 'school_id')
 .sum('Enrolled')
 .withColumn('ranking', f.row_number().over(enrolled_window))
 .orderBy('SchoolYear', f.desc('sum(Enrolled)'))
 .show())

# WHAT IS THE RANKING OF ABSTENTION  PER YEAR?

absent_window = Window.partitionBy('SchoolYear').orderBy('sum(Absent)')

(schools_df
 .groupBy('SchoolYear', 'school_id')
 .sum('Absent')
 .withColumn('ranking', f.row_number().over(absent_window))
 .orderBy('SchoolYear', 'ranking')
 .show())