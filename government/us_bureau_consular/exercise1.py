from pyspark.sql import SparkSession
from pyspark.sql import functions as f

BASE_PATH_DF = "../../data/government"

spark = (SparkSession
         .builder
         .master("local[3]")
         .appName("GovernmentStudies")
         .getOrCreate())

travels_df = (spark
              .read
              .format("json")
              .option("multiline", "true")
              .load(f"{BASE_PATH_DF}"))

print(f"Number of records: {travels_df.count()}")
travels_df.show()
