from pyspark.sql import SparkSession
from pyspark.sql import functions as f

BASEPATH = "../../data/restaurants"

spark = (SparkSession
         .builder
         .appName("NestExercise")
         .master("local[3]")
         .getOrCreate())

restaurants_business_df = (spark
                           .read
                           .format("csv")
                           .option("header", "true")
                           .option("inferSchema", "true")
                           .load(f"{BASEPATH}/businesses.CSV"))

restaurants_inspection_df = (spark
                             .read
                             .format("csv")
                             .option("header", "true")
                             .option("inferSchema", "true")
                             .load(f"{BASEPATH}/inspections.CSV"))


joined_df = restaurants_business_df.join(restaurants_inspection_df, on="business_id", how="left")
grouped_df = joined_df.groupBy("business_id", "name", "address", "city", "state", "postal_code", "latitude", "longitude", "phone_number") \
                      .agg(f.collect_list(f.struct("score", "date", "type")).alias("inspections"))


grouped_df.write.json(f"{BASEPATH}/output.json", mode="overwrite")
