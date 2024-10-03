from pyspark.sql import SparkSession
from pyspark.sql import functions as f

BASEPATH = "../../data/order"

spark = (SparkSession
         .builder
         .appName("FlatExercise")
         .master("local[3]")
         .getOrCreate())

shipment_df = (spark
               .read
               .format("json")
               .option("multiline", "true")
               .load(f"{BASEPATH}/shipment.json"))

shipment_df = (shipment_df
               .withColumn("supplier_name", f.col("supplier.name"))
               .withColumn("supplier_city", f.col("supplier.city"))
               .withColumn("supplier_state", f.col("supplier.state"))
               .withColumn("supplier_country", f.col("supplier.country"))
               .drop('supplier')
               .withColumn("customer_name", f.col("customer.name"))
               .withColumn("customer_city", f.col("customer.city"))
               .withColumn("customer_state", f.col("customer.state"))
               .withColumn("customer_country", f.col("customer.country"))
               .drop("customer")
               .withColumn("items", f.explode(f.col("books")))
               .withColumn("qty", f.col("items.qty"))
               .withColumn("title", f.col("items.title"))
               .drop("items")
               .drop("books")
               )

shipment_df.show(truncate=False)
