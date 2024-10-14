from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
import plotly.express as px

BASEPATH = "../data/economics"

spark = (SparkSession
         .builder
         .appName("Analytics Exercises")
         .master("local[3]")
         .getOrCreate())

df = (spark
      .read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(f"{BASEPATH}/grocery_restaurants_14102024.csv"))

df = (df
      .withColumn("value", f.regexp_replace(f.col("value"), ",", ".").cast("double"))
      .withColumnRenamed("sub-category", "subcategory")
      .withColumnRenamed("main-category", "maincategory")
      .withColumn("date", f.to_date("date", "dd/MM/yyyy"))
      .withColumn("week", f.weekofyear("date"))
      )

df.printSchema()
df.show()

weekly_market_expending_df = (df
                              .where(f.expr("maincategory = 'market'"))
                              .groupBy("week")
                              .agg(f.round(f.sum("value"), 2).alias("market_expending"))
                              )

windowSpec = Window.partitionBy("week")

weekly_meal_expending_df = (df
    .where(f.expr("subcategory is null and maincategory = 'meal'"))
    .groupBy("week")
    .agg(
        f.sum("value").alias("meal_total_weekly_spending"),
        f.round(f.avg("value").alias("avg_value"), 2).alias('avg_meal_expending'),
        f.max('value').alias('higher_expending')
    )
)

weekly_expending_df = (weekly_meal_expending_df
                       .join(weekly_market_expending_df, on="week")
                       )

weekly_expending_df.show()

grouped_df = (df
     .groupBy("week", "maincategory", "subcategory")
     .agg(f.round(f.sum("value"), 2).alias("expending"))
     .withColumn('category', f.concat('maincategory', f.lit(" "), f.coalesce('subcategory', f.lit(""))))
     .orderBy('week', 'category')
 )

fig = px.bar(grouped_df, x="week", y="expending", color="category",
            barmode='stack',
            title="Gastos por Semana e Categoria")
fig.show()