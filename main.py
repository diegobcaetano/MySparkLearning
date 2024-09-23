from pprint import pprint

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

from utils.Commons import evaluate_good_store
from utils.mock_df import create_simple_mock_df, easy_create_simple_mock_df

spark = (SparkSession
         .builder.master("local[3]")
         .appName("MySparkLearning")
         .enableHiveSupport()
         .getOrCreate())

spark.sql("CREATE DATABASE IF NOT EXISTS stores_db")
spark.catalog.setCurrentDatabase("stores_db")

sales_df = (spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("data/Retail_sales.csv"))

# books_df.createGlobalTempView("books_view")
# filtred_by_zip_df = spark.sql("select * from global_temp.books_view where Zip = '20853'")
# filtred_by_zip_df.show()

# defining a schema
StructType([
    StructField("SOME_DATE_COLUMN", DateType()),
    StructField("SOME_INT_COLUMN", IntegerType())
])

# defining a schema using DDL
"""
SOME_DATE_COLUMN DATE, SOME_INT_COLUMN INT, SOME_STRING_COLUMN STRING
"""

renamed_sales_df = (sales_df
                    .withColumnsRenamed({
    "Store ID": "store_id",
    "Product ID": "product_id",
    "Units Sold": "units_sold",
    "Sales Revenue (USD)": "sales_revenue_usd",
    "Store Location": "store_location",
    "Discount Percentage": "discount_percentage",
    "Product Category": "product_category",
    "Day of the Week": "day_of_week"})
                    .withColumn("sales_revenue_usd", round("sales_revenue_usd", 1)))

renamed_sales_df.printSchema()
renamed_sales_df.show()
renamed_sales_df.cache()

count_location = (renamed_sales_df
                  .where("store_location is not null")
                  .select("store_location")
                  .distinct()
                  .count())

pprint(f"Number of distinct location that we sell products: {count_location}")

count_sales_with_discount = (renamed_sales_df
                             .where("discount_percentage > 0")
                             .count())

pprint(f"Number of sales with discount: {count_sales_with_discount}")

top_locations_sales_df = (renamed_sales_df
                          .select("store_location")
                          .groupby("store_location")
                          .count()
                          .orderBy("count", ascending=False)
                          .limit(10))

top_locations_sales_df.show()

evaluate_store_udf = udf(evaluate_good_store, BooleanType())
df_analysed = top_locations_sales_df.withColumn("is_good_store", evaluate_store_udf("count"))
df_analysed.show()
(df_analysed
 .write
 .format("json")
 .mode("overwrite")
 .bucketBy(3, "store_location")
 .sortBy("store_location")
 .saveAsTable("sales_per_location"))

pprint(f"Number of partitions: {renamed_sales_df.rdd.getNumPartitions()}")
pprint(f"Number of rows per partition: {renamed_sales_df.groupBy(spark_partition_id()).count().show()}")

# (renamed_sales_df
#  .write
#  .format("json")
#  .mode("overwrite")
#  .option("path", "data_sink/json/")
#  .partitionBy("store_id", "store_location")
#  .save())

mock_df = create_simple_mock_df(spark)
mock_df.show()

mock_df2 = easy_create_simple_mock_df(spark)
mock_df2.show()
no_duplicates = mock_df2.dropDuplicates(["Name", "Age"]).sort(expr("Age asc"))
no_duplicates.show()

summary_df = (renamed_sales_df
              .where("year(Date) == 2022")
              .withColumn("year", year("Date"))
              .withColumn("month", month("Date"))
              .groupBy("product_category", "year", "month")
              .agg(sum("units_sold").alias("units_sold_total"),
                   round(sum("sales_revenue_usd"), 2).alias("revenue_total"))
              .sort("month"))

summary_df.show()

running_window = (Window
                  .partitionBy("product_category")
                  .orderBy("month")
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow))

summary_with_window_df = (summary_df
                          .withColumn("RunningTotal", sum("units_sold_total").over(running_window))
                          .drop("revenue_total"))

summary_with_window_df.show()
