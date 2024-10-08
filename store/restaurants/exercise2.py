from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType

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
grouped_df = joined_df.groupBy("business_id", "name", "address", "city", "state", "postal_code", "latitude",
                               "longitude", "phone_number") \
    .agg(f.collect_list(f.struct("score", "date", "type")).alias("inspections"))


def restaurant_could_be_opened(inspections) -> bool:
    scores = [inspection['score'] for inspection in inspections if 'score' in inspection and inspection['score'] is not None]

    if len(scores) == 0:
        return True

    media = sum(scores) / len(scores)

    if media > 92:
        return True
    return False


restaurant_could_be_opened_udf = f.udf(restaurant_could_be_opened)

grouped_df = (grouped_df
              .withColumn('can_be_opened', restaurant_could_be_opened_udf(f.col('inspections'))))

grouped_df.filter(f.expr('can_be_opened = false')).show()

grouped_df.write.json(f"{BASEPATH}/output.json", mode="overwrite")
