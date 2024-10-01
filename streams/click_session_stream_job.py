from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType

BASEPATH = "../data"

schema = StructType([
    StructField("ip", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_agent", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("metadata", StructType([
        StructField("query", StringType(), True)
    ]))
])

spark = (SparkSession
         .builder
         .appName("StreamProcessing")
         .getOrCreate())

spark.conf.set("spark.sql.streaming.checkpointLocation", f"{BASEPATH}/assets")

df = spark.readStream \
    .format("json") \
    .option("path", f"{BASEPATH}/clickstream_data") \
    .schema(schema) \
    .load()

df_processed = df.withColumn('user_query', f.col('metadata.query')) \
    .drop('user_agent')

# query = df_processed \
#     .writeStream \
#     .format("json") \
#     .option("path", f"{BASEPATH}/clickstream_data_output") \
#     .outputMode("append") \
#     .start()

query = df_processed \
    .writeStream \
    .format("console") \
    .start()

query.awaitTermination()
