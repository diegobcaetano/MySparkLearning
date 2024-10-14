from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

BASEPATH = "../data/economics"

spark = (SparkSession
         .builder
         .appName("Brazil Economics")
         .master("local[3]")
         .getOrCreate())
f.concat()
df = (spark
      .read
      .format("csv")
      .option("header", True)
      .option("delimiter", ";")
      .option("inferSchema", True)
      .load(f"{BASEPATH}/BRAZIL_CITIES.csv"))

df = (df
      .withColumn("AREA", f.regexp_replace(f.col("AREA"), "\\.", ""))
      .withColumn("AREA", f.regexp_replace(f.col("AREA"), ",", ".").cast("double"))
      )

# The highest population
(df
 .groupBy('CITY')
 .agg(f.max('IBGE_RES_POP')
      .alias('population'))
 .orderBy(f.desc('population'))
 .limit(1)
 .show())

# The biggest area
(df
 .groupBy('CITY')
 .agg(f.max('AREA')
      .alias('area'))
 .orderBy(f.desc('area'))
 .limit(1)
 .show())


# Mac Donalds density

(df
 .where(f.expr('MAC is not null'))
 .withColumn("mac_density", f.expr('1/(MAC/IBGE_POP)'))
 .select('CITY', 'IBGE_POP', 'MAC', 'mac_density')
 .show())